package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"memc-load/appsinstalled"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	normalErrorRate = 0.01
	maxRetryCount   = 5
	initRetryDelay  = 100
	maxBuffSize     = 200
	maxConnections  = 1024
)

type deviceApps struct {
	deviceType string
	deviceId   string
	lat        float64
	lon        float64
	apps       []uint32
}

func (app *deviceApps) Insert(mp *memcPool, dryRun bool) error {
	var err error
	client, err := mp.Get(app.deviceType)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", app.deviceType, app.deviceType)
	userApps := &appsinstalled.UserApps{Lat: &app.lat, Lon: &app.lat, Apps: app.apps}
	packed, err := proto.Marshal(userApps)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed proto marshal, err %s", err))
	}

	if dryRun == false {
		delay := initRetryDelay
		for attempt := 0; attempt < maxRetryCount; attempt++ {
			err = client.Set(&memcache.Item{Key: key, Value: packed})
			if err == nil {
				break
			}
			delay = 2*delay + rand.Intn(50)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}

		if err != nil {
			return errors.New(fmt.Sprintf("Failed to set value in memcache after %d attempts, err = %s", maxRetryCount, err))
		}
	}

	return nil
}

type arguments struct {
	dryRun       bool
	log          string
	pattern      string
	addresses    map[string]string
	workersCount int
	memcTimeout  int
}

type results struct {
	sync.Mutex
	processed int
	errors    int
}

func (r *results) Inc(processed int, errors int) {
	r.Lock()
	r.processed += processed
	r.errors += errors
	r.Unlock()
}

func (r *results) ErrorRate() float64 {
	if r.processed == 0 {
		return 0.0
	}
	return float64(r.errors) / float64(r.processed)
}

type memcPool struct {
	clients map[string]*memcache.Client
}

func newMemcPool(addresses map[string]string, timeout int, maxIdleConns int) *memcPool {
	clients := make(map[string]*memcache.Client)
	for name, address := range addresses {
		clients[name] = memcache.New(address)
		clients[name].Timeout = time.Duration(timeout) * time.Millisecond
		clients[name].MaxIdleConns = maxIdleConns
	}
	return &memcPool{clients: clients}
}

func (mp *memcPool) Get(name string) (*memcache.Client, error) {
	client, ok := mp.clients[name]
	if ok == false {
		return nil, errors.New(fmt.Sprintf("Client not found by name %s", name))
	}
	return client, nil
}

type memcPump struct {
	buff       []*deviceApps
	maxSize    int
	errorCount int
	dryRun     bool
	pool       *memcPool
}

func newMemcPump(pool *memcPool, maxSize int, dryRun bool) *memcPump {
	var buff []*deviceApps
	return &memcPump{
		buff:       buff,
		maxSize:    maxSize,
		errorCount: 0,
		dryRun:     dryRun,
		pool:       pool,
	}
}

func (p *memcPump) Add(app *deviceApps) {
	p.buff = append(p.buff, app)
	if len(p.buff) == p.maxSize {
		p.Drain()
	}
}

func (p *memcPump) Drain() []error {
	var output []error
	ch := make(chan error)
	for _, app := range p.buff {
		go func(a *deviceApps) {
			ch <- a.Insert(p.pool, p.dryRun)
		}(app)
	}

	for range p.buff {
		err := <-ch
		if err != nil {
			p.errorCount++
		}
		output = append(output, err)
	}
	p.buff = nil
	return output

}

func (p *memcPump) GetErrorCount() int {
	return p.errorCount
}

func parseDeviceApplications(line string) (*deviceApps, error) {
	parts := strings.Split(strings.TrimSpace(line), "\t")
	if len(parts) != 5 {
		return nil, errors.New(fmt.Sprintf("lenght of parts not equal 5. Actual lengh is %d", len(parts)))
	}
	deviceType, deviceId := parts[0], parts[1]

	if len(deviceType) == 0 {
		return nil, errors.New("device type not found")
	}

	if len(deviceId) == 0 {
		return nil, errors.New("device id not found")
	}

	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, err
	}
	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, err
	}

	var apps []uint32
	for _, app := range strings.Split(parts[4], ",") {
		app = strings.TrimSpace(app)
		appId, err := strconv.Atoi(app)
		if err != nil {
			log.Printf("cant Atoi %s", app)
			continue
		}
		apps = append(apps, uint32(appId))
	}
	return &deviceApps{
		deviceType: deviceType,
		deviceId:   deviceId,
		lat:        lat,
		lon:        lon,
		apps:       apps,
	}, nil
}

func producer(filePath string, output chan string, i int) {
	log.Printf("[%d] Processing file: %s", i, filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("[%d] error while opening file %s, err =  %s", i, filePath, err)
	}

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("[%d] error while using gzip file %s, err = %s", i, filePath, err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("[%d] Cant close file %s, err = %s", i, filePath, err)
		}
	}()

	count := 0
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		output <- scanner.Text()
		count += 1
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[%d] error while reading file %s, err = %s", i, filePath, err)
	}

	log.Printf("[%d] finish processing file %s, lines read = %d", i, filePath, count)
}

func consumer(input chan string, mp *memcPool, result *results, i int, dryRun bool) {
	var totalCount int
	var errorCount int
	pump := newMemcPump(mp, maxBuffSize, dryRun)

	for line := range input {
		totalCount += 1
		app, err := parseDeviceApplications(line)
		if err != nil {
			errorCount += 1
			log.Printf("[%d], parse err = %s", i, err)
			continue
		}
		pump.Add(app)

		if totalCount%10000 == 0 {
			log.Printf("[%d] ... processed %d lines, %d errors", i, totalCount, errorCount+pump.GetErrorCount())
		}
	}
	pump.Drain()
	errorCount += pump.GetErrorCount()

	result.Inc(totalCount, errorCount)
	log.Printf("[%d] processed %d lines, %d errors", i, totalCount, errorCount)
}

func dotRenameFile(filePath string, dryRun bool) {
	path, name := filepath.Split(filePath)
	if dryRun == false {
		err := os.Rename(filePath, filepath.Join(path, "."+name))
		if err != nil {
			log.Printf("Cant rename file %s, err = %s", filePath, err)
		}
	}
}

func setupLog(logfile string) {
	if logfile == "" {
		return
	}
	f, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Cant open log file %s, err = %s", logfile, err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Cant close file %s, err = %s", logfile, err)
		}
	}()

	log.SetOutput(io.MultiWriter(os.Stdout, f))
}

func parseArguments() arguments {
	args := arguments{}
	flag.BoolVar(&args.dryRun, "dry", true, "dry run")
	flag.StringVar(&args.log, "log", "", "log file")

	flag.StringVar(&args.pattern, "pattern", "./data/appsinstalled/[^.]*.tsv.gz", "pattern for files")

	idfa := flag.String("idfa", "127.0.0.1:33013", "idfa memcache address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "idfa memcache address")
	adid := flag.String("adid", "127.0.0.1:33015", "idfa memcache address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "idfa memcache address")

	flag.IntVar(&args.workersCount, "wc", runtime.NumCPU(), "workers count")
	flag.IntVar(&args.memcTimeout, "timeout", 500, "memcache timeout in ms")

	flag.Parse()

	args.addresses = map[string]string{
		"idfa": *idfa,
		"gaid": *gaid,
		"adid": *adid,
		"dvid": *dvid,
	}

	return args
}

func main() {

	start := time.Now()

	args := parseArguments()
	setupLog(args.log)

	log.Printf("Memc loader started with options: %+v", args)

	matches, err := filepath.Glob(args.pattern)
	if err != nil {
		log.Fatalf("Some error expected %s", err)
	}

	if len(matches) == 0 {
		log.Fatalf("No any file found for pattern %s", args.pattern)
	}

	lineChan := make(chan string)
	var workersCount = args.workersCount
	var wp sync.WaitGroup
	var wc sync.WaitGroup

	result := &results{}

	mp := newMemcPool(args.addresses, args.memcTimeout, maxConnections)

	for i, filePath := range matches {
		wp.Add(1)
		go func(f string, c chan string, i int) {
			producer(f, c, i)
			dotRenameFile(f, args.dryRun)
			wp.Done()
		}(filePath, lineChan, i)
	}

	for i := 0; i < workersCount; i++ {
		wc.Add(1)
		go func(i int) {
			consumer(lineChan, mp, result, i, args.dryRun)
			wc.Done()
		}(i)
	}

	wp.Wait()
	close(lineChan)
	wc.Wait()

	log.Printf("Total processed %d lines", result.processed)
	if result.ErrorRate() < normalErrorRate {
		log.Printf("Acceptable error rate: %f. Successfull load", result.ErrorRate())
	} else {
		log.Printf("High error rate: %f > %f. Failed load", result.ErrorRate(), normalErrorRate)
	}

	execTime := time.Since(start)
	log.Printf("Execution time = %s", execTime)
}

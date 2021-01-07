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
	"memc-load/appsinstalled"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const normalErrorRate = 0.01

type deviceApplications struct {
	deviceType string
	deviceId   string
	lat        float64
	lon        float64
	apps       []uint32
}

type arguments struct {
	dryRun bool
	log    string

	pattern string

	idfa string
	gaid string
	adid string
	dvid string

	workersCount int

	memcTimeout      int
	memcMaxIdleConns int
}

type results struct {
	sync.Mutex
	processed int
	errors int
}

func (r *results) Add (p int, e int ) {
	r.Lock()
	r.processed += p
	r.errors += e
	r.Unlock()
}

func (r *results) ErrorRate() float64 {
	if r.processed == 0 {
		return 0.0
	}
	return float64(r.errors) / float64(r.processed)
}

func parseArguments() arguments {
	args := arguments{}
	flag.BoolVar(&args.dryRun, "dry", true, "dry run")
	flag.StringVar(&args.log, "log", "", "log file")

	flag.StringVar(&args.pattern, "pattern", "./data/appsinstalled/[^.]*.tsv.gz", "pattern for files")

	flag.StringVar(&args.idfa, "idfa", "127.0.0.1:33013", "idfa memc address")
	flag.StringVar(&args.gaid, "gaid", "127.0.0.1:33014", "gaid memc address")
	flag.StringVar(&args.adid, "adid", "127.0.0.1:33015", "adid memc address")
	flag.StringVar(&args.dvid, "dvid", "127.0.0.1:33016", "dvid memc address")

	flag.IntVar(&args.workersCount, "wc", runtime.NumCPU(), "workers count")

	flag.IntVar(&args.memcTimeout, "timeout", 500, "memc timeout in ms")

	flag.Parse()

	return args
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

func parseDeviceApplications(line string) (*deviceApplications, error) {
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
	return &deviceApplications{
		deviceType: deviceType,
		deviceId:   deviceId,
		lat:        lat,
		lon:        lon,
		apps:       apps,
	}, nil
}

func insertDeviceApplications(app *deviceApplications, memcacheClient *memcache.Client, maxRetryCount int) error {
	var retryDelay = 100
	var err error

	key := fmt.Sprintf("%s:%s", app.deviceType, app.deviceType)
	userApps := &appsinstalled.UserApps{Lat: &app.lat, Lon: &app.lat, Apps: app.apps}
	packed, err := proto.Marshal(userApps)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed proto marshal, err %s", err))
	}

	for attempt := 0; attempt < maxRetryCount; attempt++ {
		err = memcacheClient.Set(&memcache.Item{Key: key, Value: packed})
		if err == nil {
			break
		}
		retryDelay = retryDelay * 2
		time.Sleep(time.Duration(retryDelay) * time.Millisecond)
	}

	if err != nil {
		return errors.New(fmt.Sprintf("Failed to set value in memcache after %d attempts, err = %s", maxRetryCount, err))
	}

	return nil
}

func producer(filePath string, out chan string, i int) {
	log.Printf("[%d] Processing file: %s", i, filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("[%d] error while opening file %s, err =  %s", i, filePath, err)
		os.Exit(1)
	}

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Printf("[%d] error while using gzip file %s, err = %s", i, filePath, err)
		os.Exit(1)
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("[%d] Cant close file %s, err = %s", i, filePath, err)
		}
	}()

	count := 0
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		out <- scanner.Text()
		count += 1
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[%d] error while reading file %s, err = %s", i, filePath, err)
		os.Exit(1)
	}

	log.Printf("[%d] finish processing file %s, lines read = %d", i, filePath, count)
}

func consumer(in chan string, memcMap map[string]*memcache.Client, result *results, i int, dryRun bool) {
	var totalCount int
	var errorCount int

	for line := range in {
		totalCount += 1
		app, err := parseDeviceApplications(line)
		if err != nil {
			errorCount += 1
			log.Printf("[%d], parse err = %s", i, err)
			continue
		}

		if dryRun == false {
			memcClient, ok := memcMap[app.deviceType]
			if ok == false {
				log.Printf("[%d] memc client not found for device type = %s", i, app.deviceType)
			}

			err = insertDeviceApplications(app, memcClient, 5)
			if err != nil {
				errorCount += 1
				log.Printf("[%d], memc err = %s", i, err)
			}
		}

		if totalCount%10000 == 0 {
			log.Printf("[%d] ... processed %d lines, %d errors", i, totalCount, errorCount)
		}
	}
	result.Add(totalCount, errorCount)
	log.Printf("[%d] processed %d lines, %d errors", i, totalCount, errorCount)
}

func newMemcacheClient(address string, maxIdleConns int, timeout int) *memcache.Client {
	client := memcache.New(address)
	client.MaxIdleConns = maxIdleConns
	client.Timeout = time.Duration(timeout) * time.Millisecond
	return client
}

func getMemcacheClientMap(args arguments) map[string]*memcache.Client {
	memcacheMap := make(map[string]*memcache.Client)
	memcMaxIdleConns := 2 * args.workersCount
	memcacheMap["idfa"] = newMemcacheClient(args.idfa, memcMaxIdleConns, args.memcTimeout)
	memcacheMap["gaid"] = newMemcacheClient(args.gaid, memcMaxIdleConns, args.memcTimeout)
	memcacheMap["adid"] = newMemcacheClient(args.adid, memcMaxIdleConns, args.memcTimeout)
	memcacheMap["dvid"] = newMemcacheClient(args.dvid, memcMaxIdleConns, args.memcTimeout)
	return memcacheMap
}

func dotRenameFile(filePath string, dryRun bool) {
	path, name := filepath.Split(filePath)
	if dryRun == false {
		err := os.Rename(filePath, filepath.Join(path, "." + name))
		if err != nil {
			log.Printf("Cant rename file %s, err = %s", filePath, err)
		}
	}
}

func main() {
	// todo readme

	start := time.Now()

	args := parseArguments()
	setupLog(args.log)

	log.Printf("Memc loader started with options: %+v", args)

	matches, err := filepath.Glob(args.pattern)
	if err != nil {
		log.Printf("Some error expected %s", err)
		os.Exit(1)
	}

	if len(matches) == 0 {
		log.Printf("No any file found for pattern %s", args.pattern)
		os.Exit(1)
	}

	lineChan := make(chan string)
	var workersCount = args.workersCount
	var wp sync.WaitGroup
	var wc sync.WaitGroup

	result := &results{}

	memcMap := getMemcacheClientMap(args)

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
			consumer(lineChan, memcMap, result, i, args.dryRun)
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

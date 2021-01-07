package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
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

type deviceApplications struct {
	deviceType string
	deviceId   string
	lat        float64
	lon        float64
	apps       []uint32
}

type arguments struct {
	dryRun bool
	log string

	pattern string

	idfa string
	gaid string
	adid string
	dvid string

	memcTimeout int
	memcMaxIdleConns int

}

func parseArguments() arguments {
	args := arguments{}
	flag.BoolVar(&args.dryRun,"dry", true, "dry run")
	flag.StringVar(&args.log, "log", "", "log file")

	flag.StringVar(&args.pattern, "pattern", "./data/appsinstalled/*.tsv.gz", "pattern for files")

	flag.StringVar(&args.idfa, "idfa", "127.0.0.1:33013", "idfa memc address")
	flag.StringVar(&args.gaid, "gaid", "127.0.0.1:33014", "gaid memc address")
	flag.StringVar(&args.adid, "adid", "127.0.0.1:33015", "adid memc address")
	flag.StringVar(&args.dvid, "dvid", "127.0.0.1:33016", "dvid memc address")

	flag.IntVar(&args.memcTimeout, "timeout", 500, "memc timeout in ms")
	flag.IntVar(&args.memcMaxIdleConns, "maxcon", 2 * runtime.NumCPU(), "memc max connecion")


	flag.Parse()

	return args
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
		log.Print(err)
		return errors.New(fmt.Sprintf("Failed to set value in memcache after %d attempts", maxRetryCount))
	}

	return nil
}

func readFile(filePath string, c chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("Processing file: %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("error while opening file: ", filePath, err)
		return
	}

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal("error while using gzip: ", filePath, err)
		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Cant close %s, err = %s", filePath, err)
		}
	}()

	count := 0
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		c <- scanner.Text()
		count += 1
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("error while reading file: ", filePath, err)
	}

	log.Printf("Finish processing file %s: %d lines read...", filePath, count)
}

func readLine(in chan string, wg *sync.WaitGroup, i int, memcMap map[string] *memcache.Client) {
	var totalCount int
	var errorCount int

	defer wg.Done()

	for line := range in {
		totalCount += 1
		app, err := parseDeviceApplications(line)
		if err != nil {
			errorCount += 1
			log.Printf("[%d], parse error %s", i, err)
			continue
		}

		memcClient, ok := memcMap[app.deviceType]
		if ok == false {
			log.Printf("[%d] memc client not found for device type = %s", i, app.deviceType)
		}
		err = insertDeviceApplications(app, memcClient, 5)
		if err != nil {
			errorCount += 1
			log.Printf("[%d], memc error %s", i, err)
		}

		if totalCount%5000 == 0 {
			log.Printf("[%d], processed %d lines, errors %d", i, totalCount, errorCount)
		}
	}
	log.Printf("[%d] processed %d lines with %d errors", i, totalCount, errorCount)
}

func newMemcacheClient(address string, maxIdleConns int, timeout int) *memcache.Client {
	client := memcache.New(address)
	client.MaxIdleConns = maxIdleConns
	client.Timeout = time.Duration(timeout) * time.Millisecond
	return client
}

func getMemcacheMap(args arguments) map[string] *memcache.Client{
	memcacheMap := make(map[string] *memcache.Client)
	memcacheMap["idfa"] = newMemcacheClient(args.idfa, args.memcMaxIdleConns, args.memcTimeout)
	memcacheMap["gaid"] = newMemcacheClient(args.gaid, args.memcMaxIdleConns, args.memcTimeout)
	memcacheMap["adid"] = newMemcacheClient(args.adid, args.memcMaxIdleConns, args.memcTimeout)
	memcacheMap["dvid"] = newMemcacheClient(args.dvid, args.memcMaxIdleConns, args.memcTimeout)
	return memcacheMap
}


func main() {
	// write log in file
	// todo calculate error rate
	// todo rename file after its done

	start := time.Now()

	args := parseArguments()
	log.Printf("Memc loader started with options: %+v", args)

	matches, err := filepath.Glob(args.pattern)
	if err != nil {
		log.Printf("Some error expected %s", err)
		return
	}

	if len(matches) == 0 {
		log.Printf("No any file found for pattern = %s", args.pattern)
		return
	}

	lineChan := make(chan string)
	var numCPU = runtime.NumCPU()
	var wp sync.WaitGroup
	var wc sync.WaitGroup

	memcMap := getMemcacheMap(args)

	for _, filePath := range matches {
		wp.Add(1)
		go readFile(filePath, lineChan, &wp)
	}

	for i := 0; i < numCPU; i++ {
		wc.Add(1)
		go readLine(lineChan, &wc, i, memcMap)
	}

	wp.Wait()
	close(lineChan)
	wc.Wait()

	execTime := time.Since(start)
	log.Printf("Execution time = %s", execTime)
}

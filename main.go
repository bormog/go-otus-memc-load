package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"memc-load/appsinstalled"
	"os"
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

func readFile(filePath string, c chan string) {
	defer close(c)

	log.Printf("Processing file: %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal("error while opening file: ", filePath, err)
		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Cant close file %s", filePath)
		}
	}()

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal("error while using gzip: ", filePath, err)
		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Cant close file %s", filePath)
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

func newMemcacheClient(address string, MaxIdleConns int, timeout int) *memcache.Client {
	client := memcache.New(address)
	client.MaxIdleConns = MaxIdleConns
	client.Timeout = time.Duration(timeout) * time.Millisecond
	return client
}

func readLine(in chan string, out chan bool,  wg *sync.WaitGroup, i int, memcacheClient *memcache.Client) {
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

		err = insertDeviceApplications(app, memcacheClient, 5)
		if err != nil {
			errorCount += 1
			log.Printf("[%d], memc error %s", i, err)
		}

		if totalCount % 10000 == 0 {
			log.Printf("[%d], processed %d lines, errors %d", i, totalCount, errorCount)
		}
	}
	out <- true
	log.Printf("[%d] processed %d lines with %d errors", i, totalCount, errorCount)
}

func main() {
	// todo arguments parser
	// todo write in memc by device type
	// todo use proto to serialize
	// todo calculate error rate
	// todo calculate execution time
	// todo work with many files
	// todo rename file afer its done

	var numCPU = runtime.NumCPU()
	var wg sync.WaitGroup
	var memcacheClient = newMemcacheClient("127.0.0.1: 33014", numCPU*2, 500)
	var filePath = "/Users/teh_momokox/python/memc_load/memc_load/test_data/.1.tsv.gz"
	//filePath = "/Users/teh_momokox/python/memc_load/memc_load/data/appsinstalled/20170929000000.tsv.gz"

	log.Printf("Number of CPU = %d", numCPU)

	lineChan := make(chan string)
	resChan := make(chan bool)

	go readFile(filePath, lineChan)

	for i := 0; i < numCPU; i++ {
		wg.Add(1)
		go readLine(lineChan, resChan, &wg, i, memcacheClient)
	}

	for i := 0; i < numCPU; i++ {
		res := <- resChan
		log.Print(res)
	}
	wg.Wait()
}

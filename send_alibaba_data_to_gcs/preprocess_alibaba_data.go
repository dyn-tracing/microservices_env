package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"errors"
    "time"
	"fmt"
	"hash/fnv"
	"io"
	"log"
    "math/big"
    "sync"
	mathrand "math/rand"
	"os"
	"sort"
	"strconv"
    "net/http"
	"strings"
    _ "net/http/pprof"

	storage "cloud.google.com/go/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.5.0"
	"google.golang.org/api/googleapi"
)

const (
	ProjectName             = "dynamic-tracing"
	TraceBucket             = "dyntraces"
	ListBucket              = "list-hashes"
    HashesByServiceBucket   = "hashes-by-service"
	PrimeNumber             = 97
	BucketSuffix            = "-quest-test"
	MicroserviceNameMapping = "names.csv"
	AnimalJSON              = "animals.csv"
	ColorsJSON              = "color_names.csv"
	MissingData             = "(?)"
	BatchSize               = 10000
)

type AliBabaSpan struct {
	timestamp               int
	trace_id                string
	rpc_id                  string
	upstream_microservice   string
	rpc_type                string
	ali_interface           string
	downstream_microservice string
	response_time           int
}

type spanStr struct {
	parent  string
	id      string
	service string
}

type TimeWithTrace struct {
	timestamp int
	trace     ptrace.Traces
}

type Exempted struct {
    total int
    exemptedCycle int
    exemptedFrag int
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func getAnimalNames() []string {
	f, err := os.Open(AnimalJSON)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	animals := make([]string, 0)
	csvReader := csv.NewReader(f)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		withoutSpaces := strings.ReplaceAll(row[0], " ", "")
		animals = append(animals, withoutSpaces)
	}
	return animals
}

func getColorNames() []string {
	f, err := os.Open(ColorsJSON)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	colors := make([]string, 0)
	csvReader := csv.NewReader(f)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		withoutSpaces := strings.ReplaceAll(row[0], " ", "")
		colors = append(colors, withoutSpaces)
	}
	return colors
}

func getNewEntry(microservice_name_mapping map[string]string, animalNames []string, colorNames []string, animal_color_to_hash map[string]string) string {
	found := false
	for !found {
		randomAnimalIndex := mathrand.Intn(len(animalNames))
		randomColorIndex := mathrand.Intn(len(colorNames))
		possibleName := animalNames[randomAnimalIndex] + colorNames[randomColorIndex]
		if _, ok := animal_color_to_hash[possibleName]; ok {
			// sad, we've already tried this one
			continue
		} else {
			return possibleName
		}
	}
	return ""
}

func createAliBabaSpan(row []string, microservice_name_mapping map[string]string,
	animalNames []string, colorNames []string,
	animalColorToHashName map[string]string) AliBabaSpan {
	var newSpan AliBabaSpan
	// if already exists in map, great
	if val, ok := microservice_name_mapping[row[4]]; ok {
		newSpan.upstream_microservice = val
	} else if row[4] == MissingData {
		newSpan.upstream_microservice = row[4]
	} else {
		// create new entry in map
		newEntry := getNewEntry(microservice_name_mapping, animalNames, colorNames, animalColorToHashName)
		microservice_name_mapping[row[4]] = newEntry
		animalColorToHashName[newEntry] = row[4]
		newSpan.upstream_microservice = newEntry
	}
	if val, ok := microservice_name_mapping[row[6]]; ok {
		newSpan.downstream_microservice = val
	} else if row[6] == MissingData {
		newSpan.downstream_microservice = row[6]
	} else {
		// create new entry in map
		newEntry := getNewEntry(microservice_name_mapping, animalNames, colorNames, animalColorToHashName)
		microservice_name_mapping[row[6]] = newEntry
		animalColorToHashName[newEntry] = row[6]
		newSpan.downstream_microservice = newEntry
	}
	newSpan.trace_id = row[1]
	newSpan.timestamp, _ = strconv.Atoi(row[2])
    // We want realistic timestamps, so just adding time as of Dec 7th to get offsets in the 12 hour Alibaba period
	newSpan.timestamp = newSpan.timestamp + 1670427276
	newSpan.rpc_id = row[3]
	newSpan.rpc_type = row[5]
	newSpan.ali_interface = row[7]
	newSpan.response_time, _ = strconv.Atoi(row[8])
	return newSpan
}

func importNameMapping() map[string]string {
	_, err := os.Stat(MicroserviceNameMapping)
	if os.IsNotExist(err) {
		return make(map[string]string)
	}
	f, err := os.Open(MicroserviceNameMapping)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	mapping := make(map[string]string)
	csvReader := csv.NewReader(f)
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		mapping[row[0]] = row[1]
	}
	return mapping
}

func importAliBabaData(filename string, filenum int, microservice_name_mapping map[string]string) map[string][]AliBabaSpan {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	mapping := make(map[string][]AliBabaSpan)
	animalNames := getAnimalNames()
	colorNames := getColorNames()

	// create mapping from fake name to real hash
	animalColorToHashName := make(map[string]string)
	for hash, color := range microservice_name_mapping {
		animalColorToHashName[color] = hash
	}

    first := true
	csvReader := csv.NewReader(f)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
        if first {
            // ignore first header line
            first = false
            continue
        }
		newSpan := createAliBabaSpan(rec, microservice_name_mapping, animalNames, colorNames, animalColorToHashName)
        if newSpan.rpc_id != "0" {
		    mapping[newSpan.trace_id] = append(mapping[newSpan.trace_id], newSpan)
        }
	}
    // Write out new microservice name mapping
    mapping_file, err := os.Create(MicroserviceNameMapping)
    defer mapping_file.Close()
    if err != nil {
        log.Fatalln("failed to open file", err)
    }
    w := csv.NewWriter(mapping_file)
    defer w.Flush()
    for service, pseudonym := range(microservice_name_mapping) {
        to_write := []string{service, pseudonym}
        if err := w.Write(to_write); err != nil {
            log.Fatalln("error writing record to file", err)
        }
    }

	return mapping
}

func bytesTo16Bytes(input []byte) [16]byte {
	tmpSlice := [16]byte{}
	for i, b := range input {
		tmpSlice[i] = b
	}
	return tmpSlice
}

func bytesTo8Bytes(input []byte) [8]byte {
	tmpSlice := [8]byte{}
	for i, b := range input {
		tmpSlice[i] = b
	}
	return tmpSlice
}

func spanBucketExists(ctx context.Context, serviceName string, isService bool, client *storage.Client) error {
	var storageClassAndLocation storage.BucketAttrs
	if isService {
		labels := make(map[string]string)
		labels["bucket_type"] = "microservice"
		storageClassAndLocation = storage.BucketAttrs{
			StorageClass: "STANDARD",
			Location:     "us-central1",
			LocationType: "region",
			Labels:       labels,
		}
	} else {
		storageClassAndLocation = storage.BucketAttrs{
			StorageClass: "STANDARD",
			Location:     "us-central1",
			LocationType: "region",
		}
	}
	bkt := client.Bucket(serviceNameToBucketName(serviceName, BucketSuffix))
	_, err := bkt.Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		if crErr := bkt.Create(ctx, ProjectName, &storageClassAndLocation); crErr != nil {
            println("had a problem")
			var e *googleapi.Error
			if ok := errors.As(crErr, &e); ok {
				if e.Code != 409 { // 409s mean some other thread created the bucket in the meantime;  ignore it
                    println("failed creating bucket: ", crErr)
					return fmt.Errorf("failed creating bucket: %w", crErr)
				} else {
					println("got 409")
					return nil
				}

			}
		}
	} else if err != nil {
		return fmt.Errorf("failed getting bucket attributes: %w", err)
	}
	_, err = bkt.Attrs(ctx)
    if err == storage.ErrBucketNotExist {
        println("it says bucket doesn't exist when I just created it")
        return err
    }
	return err
}

func isCyclic(aliBabaSpans []AliBabaSpan, v int, upstreamMap map[string][]int, visited map[int]bool, recStack []bool) bool {

    if visited[v] == false {
        visited[v] = true;
        recStack[v] = true;

        for _, child := range upstreamMap[aliBabaSpans[v].downstream_microservice] {
            if aliBabaSpans[child].upstream_microservice == aliBabaSpans[child].downstream_microservice {
                continue
            }
            if aliBabaSpans[child].downstream_microservice == MissingData {
                continue
            }
            if visited[child] == false && isCyclic(aliBabaSpans, child, upstreamMap, visited, recStack) {
                return true
            } else if recStack[child] {
                return true
            }
        }
    }
    recStack[v] = false;
    return false;
}

func makePData(aliBabaSpans []AliBabaSpan) TimeWithTrace {
	traces := ptrace.NewTraces()
	earliest_time := aliBabaSpans[0].timestamp
	upstreamMap := make(map[string][]int)
    statusCode := []string{"200", "400", "402", "404", "302", "500", "501", "540"}

	for _, aliBabaSpan := range aliBabaSpans {
		if aliBabaSpan.timestamp < earliest_time {
			earliest_time = aliBabaSpan.timestamp
		}

		batch := traces.ResourceSpans().AppendEmpty()
        // If we have a span sans parent, and it's not root, we can't reassemble
        if aliBabaSpan.upstream_microservice == MissingData && aliBabaSpan.rpc_id != "0.1" && aliBabaSpan.rpc_id != "0.1.1" {
            continue
        }
		batch.Resource().Attributes().PutStr("service.name", aliBabaSpan.downstream_microservice) // what if dm is missing ?
		batch.Resource().Attributes().PutStr("upstream.name", aliBabaSpan.upstream_microservice)  // what if dm is missing ?
		batch.Resource().Attributes().PutStr("rpc.id", aliBabaSpan.rpc_id)
		ils := batch.ScopeSpans().AppendEmpty()
		span := ils.Spans().AppendEmpty()
        span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(int64(aliBabaSpan.timestamp), 0)))
		randomStatus := mathrand.Intn(len(statusCode))
        span.Attributes().PutStr("http.status_code", statusCode[randomStatus])
        span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(int64(aliBabaSpan.timestamp), 0)))

		trace_id_bytes, err := hex.DecodeString(aliBabaSpan.trace_id)
		trace_id := pcommon.TraceID(bytesTo16Bytes(trace_id_bytes))
		span.SetTraceID(trace_id)
		_ = err
	}

	root_span_index := -1
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		if sn, ok := traces.ResourceSpans().At(i).Resource().Attributes().Get("upstream.name"); ok {
			upstreamMap[sn.AsString()] = append(upstreamMap[sn.AsString()], i)
		}

		if rpc_id, ok := traces.ResourceSpans().At(i).Resource().Attributes().Get("rpc.id"); ok {
			if rpc_id.AsString() == "0.1" {
				root_span_index = i
			} else if rpc_id.AsString() == "0.1.1" && root_span_index == -1 {
				root_span_index = i
			}
		}
	}

	if root_span_index == -1 {
		return TimeWithTrace{}
	}

	queue := make([]int, 0)
	queue = append(queue, root_span_index)
	visited := make(map[int]bool)

    cyclic_visited := make(map[int]bool)
    recStack := make([]bool, len(aliBabaSpans))
    for i := 0; i<len(aliBabaSpans); i++ {
        cyclic_visited[i] = false;
        recStack[i] = false;
    }
    if (isCyclic(aliBabaSpans, root_span_index, upstreamMap, cyclic_visited, recStack)) {
        // :( cyclic
        return TimeWithTrace{-1, ptrace.NewTraces()}
    }

	for {
		if len(queue) < 1 {
			break
		}
        if len(queue) > len(aliBabaSpans) {
            os.Exit(0)
        }
		top := queue[0]

		visited[top] = true

		queue = queue[1:]
		dm := aliBabaSpans[top].downstream_microservice
		nextLevel := upstreamMap[dm]

		if dm == aliBabaSpans[top].upstream_microservice || dm == MissingData {
			nextLevel = []int{}
		}

		raw_span_id := make([]byte, 8)
		rand.Read(raw_span_id)
		span_id := pcommon.SpanID(bytesTo8Bytes(raw_span_id))

		traces.ResourceSpans().At(top).ScopeSpans().At(0).Spans().At(0).SetSpanID(span_id)

		for _, ele := range nextLevel {
			traces.ResourceSpans().At(ele).ScopeSpans().At(0).Spans().At(0).SetParentSpanID(span_id)

            // this speeds up the iteration since we know there aren't any cycles
            if ele == top {
                println("ELE == TOP")
            }
            in_queue := false
            for _, queue_ele := range queue {
                if ele == queue_ele {
                    in_queue = true
                    break
                }
            }
            if !in_queue {
			    queue = append(queue, ele)
            }
		}
		// log.Println(".")
	}

	// Unreachibility thin'
	for ind := 0; ind < traces.ResourceSpans().Len(); ind++ {
		if _, ok := visited[ind]; !ok {
            return TimeWithTrace{-2, ptrace.NewTraces()}
		}
	}
	return TimeWithTrace{earliest_time, traces}
}

func serviceNameToBucketName(service string, suffix string) string {
	bucketID := strings.ReplaceAll(service, ".", "")
	bucketID = strings.ReplaceAll(bucketID, "/", "")
	bucketID = strings.ReplaceAll(bucketID, "google", "")
	bucketID = strings.ReplaceAll(bucketID, "_", "")
	bucketID = strings.ToLower(bucketID)
	return bucketID + suffix
}

func createBuckets(ctx context.Context, traces []TimeWithTrace, client *storage.Client) {
    // check this
    var wg sync.WaitGroup
    wg.Add(5)
	go func(ctx context.Context, client *storage.Client, wg *sync.WaitGroup){
        spanBucketExists(ctx, TraceBucket, false, client)
        wg.Done()
    }(ctx, client, &wg)
	go func(ctx context.Context, client *storage.Client, wg *sync.WaitGroup){
	    spanBucketExists(ctx, "tracehashes", false, client)
        wg.Done()
    }(ctx, client, &wg)
	go func(ctx context.Context, client *storage.Client, wg *sync.WaitGroup){
        spanBucketExists(ctx, "microservices", false, client)
        wg.Done()
    }(ctx, client, &wg)
	go func(ctx context.Context, client *storage.Client, wg *sync.WaitGroup){
        spanBucketExists(ctx, ListBucket, false, client)
        wg.Done()
    }(ctx, client, &wg)
	go func(ctx context.Context, client *storage.Client, wg *sync.WaitGroup){
        spanBucketExists(ctx, HashesByServiceBucket, false, client)
        wg.Done()
    }(ctx, client, &wg)

    wg.Wait()
}

func sendBatchSpansWorker(ctx context.Context, resourceNameToSpans map[string]ptrace.Traces,
    batch_name string, client *storage.Client, jobs <-chan string, results chan<- int) {
	tracesMarshaler := &ptrace.ProtoMarshaler{}
    for resource := range jobs {
        resource_final := resource
        if resource_final == MissingData {
            resource_final = "MissingService"
        }
        spans := resourceNameToSpans[resource]
        bucketName := serviceNameToBucketName("microservices", BucketSuffix)
        bkt := client.Bucket(bucketName)

        buffer, err := tracesMarshaler.MarshalTraces(spans)
        if err != nil {
            print("could not marshal traces")
        }
        obj := bkt.Object(resource_final + BucketSuffix + "/" + batch_name)
        ctx := context.Background()
        writer := obj.NewWriter(ctx)
        if _, err := writer.Write(buffer); err != nil {
            println("failed writing the span object: ", err.Error())
            println("when we are writing object ", batch_name, " in bucket ", bucketName)
        }
        if err := writer.Close(); err != nil {
            println("failed closing the span object: ", err.Error())
            println("when we are writing object ", batch_name, " in bucket ", bucketName)
        }
        results <- 1
    }
}

func sendBatchSpansToStorage(ctx context.Context, traces []TimeWithTrace, batch_name string, client *storage.Client) error {
	resourceNameToSpans := make(map[string]ptrace.Traces)
	for time_with_trace := range traces {
		span := traces[time_with_trace].trace
		for i := 0; i < span.ResourceSpans().Len(); i++ {
			if sn, ok := span.ResourceSpans().At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
				if _, ok := resourceNameToSpans[sn.AsString()]; ok {
					span.ResourceSpans().At(i).CopyTo(resourceNameToSpans[sn.AsString()].ResourceSpans().AppendEmpty())
				} else {
					newOrganizedSpans := ptrace.NewTraces()
					span.ResourceSpans().At(i).CopyTo(newOrganizedSpans.ResourceSpans().AppendEmpty())
					resourceNameToSpans[sn.AsString()] = newOrganizedSpans
				}
			} else {
				println("couldn't get service name")
			}
		}
	}


	// 3. Send each resource's spans to storage
    numJobs := len(resourceNameToSpans)
    jobs := make(chan string, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 150;
    for w := 1; w <= numWorkers; w++ {
        go sendBatchSpansWorker(ctx, resourceNameToSpans, batch_name, client, jobs, results)
    }
	for resource, _ := range resourceNameToSpans {
        jobs <- resource
	}
    close(jobs)

    for a := 1; a <= numJobs; a++ {
        <-results
    }
	return nil
}

func hashTrace(ctx context.Context, spans []spanStr) (map[*spanStr]int, int) {
	// Don't need to do all the computation if you just have one span
	spanToHash := make(map[*spanStr]int)
	if len(spans) == 1 {
		spanToHash[&spans[0]] = int(hash(spans[0].service)) * PrimeNumber
		return spanToHash, spanToHash[&spans[0]]
	}
	// 1. Find root
	var root int
	for i := 0; i < len(spans); i++ {
		if spans[i].parent == "" {
			root = i
		} else if spans[i].parent == "ffffffffffffffff" {
			spans[i].parent = ""
			root = i
		}
	}

	// 2. To do hashes, you need two mappings:  parent to child, and level to spans at that level
	// 2. Need to make three mappings:
	//    a. parent to child
	//    b. level to spans
	//    c. span to hash
	parentToChild := make(map[*spanStr][]*spanStr)
	childToParent := make(map[*spanStr]*spanStr)
	levelToSpans := make(map[int][]*spanStr)
	spanToLevel := make(map[*spanStr]int)
	// 2a.  Fill out parent-child mappings
	// is there a better way to traverse this?
	// you could probably do a stack of visited and unvisited nodes
	// but to wade through which are visited/unvisited each time it would be O(n) anyway, so I don't think
	// you lose any efficiency in practice through the O(n^2) solution
	for i := 0; i < len(spans); i++ {
		for j := 0; j < len(spans); j++ {
			if i != j && spans[j].parent == spans[i].id {
				parentToChild[&spans[i]] = append(parentToChild[&spans[i]], &spans[j])
				childToParent[&spans[j]] = &spans[i]
			}
		}
	}

	// 2b. Fill out level-spanID mappings

	var maxLevel int
	maxLevel = 0

	levelToSpans[0] = append(levelToSpans[0], &spans[root])
	spanToLevel[&spans[root]] = 0

	toAssignLevel := make([]*spanStr, 0)
	toAssignLevel = append(toAssignLevel, parentToChild[&spans[root]]...)
	for len(toAssignLevel) > 0 {
		// dequeue
		spanToAssign := toAssignLevel[0]
		toAssignLevel = toAssignLevel[1:]
		// enqueue your children
		toAssignLevel = append(toAssignLevel, parentToChild[spanToAssign]...)
		// parent is guaranteed in spanToLevel
		parentLevel := spanToLevel[childToParent[spanToAssign]]
		// my level is one more than my parent's
		spanToLevel[spanToAssign] = parentLevel + 1
		levelToSpans[parentLevel+1] = append(levelToSpans[parentLevel+1], spanToAssign)
		if parentLevel+1 > maxLevel {
			maxLevel = parentLevel + 1
		}
	}
	// 2c.  Use previous two mappings to fill out span ID to hash mappings
	for i := int(maxLevel); i >= 0; i-- {
		// for each level, create hash
		for j := 0; j < len(levelToSpans[i]); j++ {
			span := levelToSpans[i][j]
			spanHash := int(hash(span.service)) + i*PrimeNumber
			// now add all your children
			for k := 0; k < len(parentToChild[span]); k++ {
				spanHash += spanToHash[parentToChild[span][k]]
			}
			spanToHash[span] = spanHash
		}

	}
	return spanToHash, spanToHash[&spans[root]]
}


func sendTraceIDsForHashWorker(ctx context.Context, hashToTraceID map[int][]string,
    batch_name string, client* storage.Client,
    jobs <-chan int, results chan<- int) {
	bkt := client.Bucket(serviceNameToBucketName("tracehashes", BucketSuffix))

    for hash := range jobs {
        traces := hashToTraceID[hash]
		traceIDs := dataBuffer{}
		for i := 0; i < len(traces); i++ {
			traceIDs.logEntry("%s", traces[i])
		}
		obj := bkt.Object(strconv.FormatUint(uint64(hash), 10) + "/" + batch_name)
		w := obj.NewWriter(ctx)
		if _, err := w.Write(traceIDs.buf.Bytes()); err != nil {
			println(fmt.Errorf("failed creating the object: %w", err))
			println("error: ", err.Error())
		}
		if err := w.Close(); err != nil {
			println(fmt.Errorf("failed closing the hash object in bucket %s: %w", strconv.FormatUint(uint64(hash), 10)+"/"+batch_name, err))
			println("error: ", err.Error())
		}
        results <- 1
    }
}

func sendHashToTraceIDMapping(ctx context.Context, hashToTraceID map[int][]string, batch_name string, client *storage.Client) {

    numJobs := len(hashToTraceID)
    jobs := make(chan int, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 100

    for w := 1; w <= numWorkers; w++ {
        go sendTraceIDsForHashWorker(ctx, hashToTraceID, batch_name, client, jobs, results)
    }
    //computed_time := time.Now()

    for hash, _ := range hashToTraceID {
        jobs <- hash
    }
    close(jobs)

    for a := 1; a <= numJobs; a++ {
        <-results
    }
    //fmt.Println("time for send hash to trace ID mapping to actually run: ", time.Since(computed_time))
}

func writeMicroserviceToHashMappingWorker(ctx context.Context, hash int, client *storage.Client,
    jobs <-chan string, results chan<- int) {
	service_bkt := client.Bucket(serviceNameToBucketName(HashesByServiceBucket, BucketSuffix))
	emptyBuf := dataBuffer{}
    for service := range jobs {
        service_obj := service_bkt.Object(service + "/" + strconv.FormatUint(uint64(hash), 10))
        _, err := service_obj.Attrs(ctx)
        if err == storage.ErrObjectNotExist {
            service_writer := service_obj.NewWriter(ctx)
            if _, err := service_writer.Write(emptyBuf.buf.Bytes()); err != nil {
                println(fmt.Errorf("failed writing the hash by service object in bucket %s: %w",
                    strconv.FormatUint(uint64(hash), 10), err))
                println("failed in writing")
                println("error: ", err.Error())
            }
            if err := service_writer.Close(); err != nil {
                println(fmt.Errorf("failed closing the hash by service object in bucket %s: %w",
                    strconv.FormatUint(uint64(hash), 10), err))
                println("failed in closing")
                println("error: ", err.Error())
            }
        }
    }
    results <- 1
}

func writeHashExemplarsWorker(ctx context.Context, hashToStructure map[int]dataBuffer,
    hashToServices map[int][]string, batch_name string, client *storage.Client, jobs <-chan int, results chan<- int) {
	list_bkt := client.Bucket(serviceNameToBucketName(ListBucket, BucketSuffix))
    for hash := range jobs {
        structure := hashToStructure[hash]
		// 1. Does object exist?
		obj := list_bkt.Object(strconv.FormatUint(uint64(hash), 10))
		_, err := obj.Attrs(ctx)
		if err == storage.ErrObjectNotExist {
			// 2. If doesn't exist, create it
			w_list := obj.NewWriter(ctx)
			if _, err := w_list.Write(structure.buf.Bytes()); err != nil {
				println(fmt.Errorf("failed creating the list object: %w", err))
				println("error: ", err.Error())
			}
			if err := w_list.Close(); err != nil {
				println(fmt.Errorf("failed closing the list hash object in bucket %s: %w", strconv.FormatUint(uint64(hash), 10), err))
				println("error2: ", err.Error())
				fmt.Errorf("failed closing the list hash object in bucket %s: %w", strconv.FormatUint(uint64(hash), 10), err)

				log.Fatal(err)
			}
            // 3. Then, create microservice -> hash mapping for this hash
            numJobs := len(hashToServices[hash])
            inner_jobs := make(chan string, numJobs)
            inner_results := make(chan int, numJobs)

            numWorkers := 20
            for w := 1; w <= numWorkers; w++ {
                go writeMicroserviceToHashMappingWorker(ctx, hash, client, inner_jobs, inner_results)
            }
            for _, service := range hashToServices[hash] {
                inner_jobs <- service
            }
            close(inner_jobs)
            for a := 1; a <= numJobs; a ++ {
                <-inner_results
            }

            // Results counts how many new exemplars
            results <- 1
		} else {
            results <- 0
        }
    }
}

func writeHashExemplarsAndHashByMicroservice(ctx context.Context, hashToStructure map[int]dataBuffer,
    hashToServices map[int][]string, batch_name string, client *storage.Client) int {
    numJobs := len(hashToStructure)
    jobs := make(chan int, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 100

    for w := 1; w <= numWorkers; w++ {
        go writeHashExemplarsWorker(ctx, hashToStructure, hashToServices, batch_name, client, jobs, results)
    }

    for hash, _ := range hashToStructure {
        jobs <- hash
    }
    close(jobs)

    totalNew := 0
    for a := 1; a <= numJobs; a++ {
        totalNew += <-results
    }
    return totalNew
}

func computeHashesAndTraceStructToStorage(ctx context.Context, traces []TimeWithTrace, batch_name string, client *storage.Client) (error, int) {
	// 1. Collect the trace structures in traceStructBuf, and a map of hashes to traceIDs
    //start_time := time.Now()
	traceStructBuf := dataBuffer{}
	hashToTraceID := make(map[int][]string)
	hashToStructure := make(map[int]dataBuffer)
    hashToServices := make(map[int][]string)
	for _, trace := range traces {
		traceID := trace.trace.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()
		var sp []spanStr
		traceStructBuf.logEntry("Trace ID: %s:", traceID.HexString())
		for i := 0; i < trace.trace.ResourceSpans().Len(); i++ {
			span := trace.trace.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			parent := span.ParentSpanID().HexString()
			spanID := span.SpanID().HexString()
			if sn, ok := trace.trace.ResourceSpans().At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
                service_name := sn.AsString()
                if service_name == MissingData {
                    service_name = "MissingService"
                }
				sp = append(sp, spanStr{
					parent:  parent,
					id:      spanID,
					service: service_name})

			}
		}
		hashmap, hash := hashTrace(ctx, sp)
		// If the hash to structure has not yet been filled, fill it.
		hashFilled := true
		structBuf := dataBuffer{}
		if _, ok := hashToStructure[hash]; !ok {
			hashFilled = false
			structBuf.logEntry("Trace ID: %s:", traceID.HexString())
		}
		for i := 0; i < len(sp); i++ {
			traceStructBuf.logEntry("%s:%s:%s:%s", sp[i].parent, sp[i].id, sp[i].service,
				strconv.FormatUint(uint64(hashmap[&sp[i]]), 10))
			if !hashFilled {
				structBuf.logEntry("%s:%s:%s:%s", sp[i].parent, sp[i].id, sp[i].service,
					strconv.FormatUint(uint64(hashmap[&sp[i]]), 10))
			}

		}
		hashToTraceID[hash] = append(hashToTraceID[hash], traceID.HexString())
		if !hashFilled {
			hashToStructure[hash] = structBuf
            var services []string
            for i :=0; i < len(sp); i++ {
                services = append(services, sp[i].service)
            }
            hashToServices[hash] = services
		}
	}
    //fmt.Println("time to compute all hashes: ", time.Since(start_time))
    //computed_time := time.Now()


	// 2. Put the trace structure buffer in storage
	trace_bkt := client.Bucket(serviceNameToBucketName(TraceBucket, BucketSuffix))

	trace_obj := trace_bkt.Object(batch_name)
	w_trace := trace_obj.NewWriter(ctx)
	if _, err := w_trace.Write([]byte(traceStructBuf.buf.Bytes())); err != nil {
		return fmt.Errorf("failed creating the trace object: %w", err), 0
	}
	if err := w_trace.Close(); err != nil {
		return fmt.Errorf("failed closing the trace object %w", err), 0
	}
    //fmt.Println("time to send trace struct buffer: ", time.Since(computed_time))

    //before_hash_mapping_time := time.Now()

	// 3. Put the hash to trace ID mapping in storage
    sendHashToTraceIDMapping(ctx, hashToTraceID, batch_name, client)
    //fmt.Println("time to send hash to trace ID mapping: ", time.Since(before_hash_mapping_time))

    //last_time := time.Now()
    numHashExemplars := writeHashExemplarsAndHashByMicroservice(ctx, hashToStructure, hashToServices, batch_name, client)
    //fmt.Println("time to write hash exemplars: ", time.Since(last_time))

	return nil, numHashExemplars
}

func createPDataWorker(traceIDToAliBabaSpans map[string][]AliBabaSpan,
    jobs <-chan string, results chan<- TimeWithTrace) {
    for traceID := range jobs {
        results <- makePData(traceIDToAliBabaSpans[traceID])
    }
}

func createPDataTraces(traceIDToAliBabaSpans map[string][]AliBabaSpan) ([]TimeWithTrace, Exempted){
	pdataTraces := make([]TimeWithTrace, 0, len(traceIDToAliBabaSpans))
	empty := TimeWithTrace{}
    totalTraces := 0
    cyclicExemptedTraces := 0
    fragExemptedTraces := 0

    numJobs := len(traceIDToAliBabaSpans)
    jobs := make(chan string, numJobs)
    results := make(chan TimeWithTrace, numJobs)

    numWorkers := 32 // Should be number of cores; this is purely CPU-bound work
    for w := 1; w <= numWorkers; w++ {
        go createPDataWorker(traceIDToAliBabaSpans, jobs, results)
    }
	for traceID, _ := range traceIDToAliBabaSpans {
        jobs <- traceID
    }
    close(jobs)

    for a := 1; a <= numJobs; a++ {
        timeAndpdataSpans := <-results
        totalTraces += 1
		if timeAndpdataSpans != empty && timeAndpdataSpans.timestamp != -1 && timeAndpdataSpans.timestamp != -2 {
			pdataTraces = append(pdataTraces, timeAndpdataSpans)
		} else if timeAndpdataSpans.timestamp == -1 {
            cyclicExemptedTraces += 1
		} else if timeAndpdataSpans.timestamp == -2 {
            fragExemptedTraces += 1
        }
	}

    println("total traces: ", totalTraces)
    println("exempted traces (cyclic): ", cyclicExemptedTraces)
    println("exempted traces (frag): ", fragExemptedTraces)
    to_return := Exempted{totalTraces, cyclicExemptedTraces, fragExemptedTraces}

    return pdataTraces, to_return
}

func getTotalBytesWorker(pdataTraces []TimeWithTrace, jobs <-chan int, results chan<- int) {
	tracesMarshaler := &ptrace.ProtoMarshaler{}
    for index := range jobs {
        buffer, err := tracesMarshaler.MarshalTraces(pdataTraces[index].trace)
        if err != nil {
            print("could not marshal traces")
        }
        results <- len(buffer)
    }
}

func getTotalBytes(pdataTraces []TimeWithTrace) int {
    totalBytes := 0
    numJobs := len(pdataTraces)
    jobs := make(chan int, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 32 // Should be number of cores; this is purely CPU-bound work
    for w := 1; w <= numWorkers; w++ {
        go getTotalBytesWorker(pdataTraces, jobs, results)
    }
	for i := 0; i < len(pdataTraces); i++ {
        jobs <- i
    }
    close(jobs)

    for a := 1; a <= numJobs; a++ {
        totalBytes += <-results
    }
    return totalBytes
}

func process_file(filename string) Exempted {
	// determine if name mapping file exists
	microservice_hash_to_name := importNameMapping()
	traceIDToAliBabaSpans := importAliBabaData(filename, 1, microservice_hash_to_name)
    //start_time := time.Now()
    pdataTraces, to_return := createPDataTraces(traceIDToAliBabaSpans)
    //fmt.Println("time to create pdata spans: ", time.Since(start_time))

    //sorting_time := time.Now()
	// Then organize the spans by time, and batch them.
	sort.Slice(pdataTraces, func(i, j int) bool {
		return pdataTraces[i].timestamp < pdataTraces[j].timestamp
	})
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		print("could not create gcs client")
		os.Exit(0)
	}
    //fmt.Println("time to sort and create client: ", time.Since(sorting_time))
    //buckets_exist_time := time.Now()

    // Make sure all buckets exist
    createBuckets(ctx, pdataTraces, client)
    //fmt.Println("time to make sure buckets exist: ", time.Since(buckets_exist_time))


	// Now, we batch.
    println("done creating buckets")
    //start_sending_time := time.Now()

    var wg sync.WaitGroup
	j := 0

    newHashesChannels := make(chan int, len(pdataTraces))

	for j < len(pdataTraces) {
		start := j
		end := start + BatchSize
		if end >= len(pdataTraces) {
			end = len(pdataTraces)
		}

		// Name of this batch is...
		random, _ := rand.Int(rand.Reader, big.NewInt(100000000))
		int_hash := strconv.FormatUint(uint64(random.Int64()), 10)
		if len(int_hash) == 1 {
			int_hash = "0" + int_hash
		}
		batch_name := int_hash[0:2] + "-" +
			strconv.Itoa(pdataTraces[start].timestamp) + "-" +
			strconv.Itoa(pdataTraces[end-1].timestamp)
        wg.Add(2)
		go func (ctx context.Context, pdataTraces []TimeWithTrace, batch_name string, client *storage.Client, start int, end int, wg *sync.WaitGroup) {
            sendBatchSpansToStorage(ctx, pdataTraces[start:end], batch_name, client)
            wg.Done()
        }(ctx, pdataTraces, batch_name, client, start, end, &wg)

		go func (ctx context.Context, pdataTraces []TimeWithTrace, batch_name string, client *storage.Client, start int, end int, wg *sync.WaitGroup) {
		    err, numHashes := computeHashesAndTraceStructToStorage(ctx, pdataTraces[start:end], batch_name, client)
            if err != nil {
                print("error in compute hashes and trace struct to storage")
            }
            newHashesChannels <- numHashes
            wg.Done()
        }(ctx, pdataTraces, batch_name, client, start, end, &wg)
		j += BatchSize
	}
    wg.Wait()
    close(newHashesChannels)
    totalNewHashes := 0
    for i := 0; i< len(pdataTraces); i++ {
        totalNewHashes += <-newHashesChannels
    }
    println("total new hashes: ", totalNewHashes)
    //fmt.Println("time to send all data to GCS: ", time.Since(start_sending_time))
    totalBytes := getTotalBytes(pdataTraces)
    println("Total bytes: ", totalBytes)
    return to_return
}

func main() {
	if len(os.Args) != 2 {
		println("usage: ./preprocess_alibaba_data filename")
		os.Exit(0)
	}

	filename := os.Args[1]
    exempted_total := Exempted{0,0,0}
    go func() {
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()
    if filename == "MSCallGraph" {
        for i:=1; i<=2; i++ {
            new_file_name := filename+"_"+strconv.Itoa(i)+".csv"
            println("processing file: ", new_file_name)
            exempted_result := process_file(new_file_name)
            exempted_total.total += exempted_result.total
            exempted_total.exemptedCycle += exempted_result.exemptedCycle
            exempted_total.exemptedFrag += exempted_result.exemptedFrag
        }
        println("TOTAL:")
        println("total traces: ", exempted_total.total)
        println("total exempted for cycles: ", exempted_total.exemptedCycle)
        println("total exemptedfor frag: ", exempted_total.exemptedFrag)
    } else {
        process_file(filename)
    }

}

// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecloudstorageexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/googlecloudstorageexporter"

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
    "strings"
    "strconv"
    "time"
    "errors"
    "crypto/rand"
    "os"
    "math/big"
    "sync"

    storage "cloud.google.com/go/storage"
    conventions "go.opentelemetry.io/collector/semconv/v1.5.0"
    "google.golang.org/api/googleapi"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
    "hash/fnv"
)

const name = "googlecloudstorage"
const trace_bucket = "dyntraces"
const HashesByServiceBucket = "hashes-by-service"
const ListBucket = "list-hashes"
const primeNumber = 97
var hashNumber string

type storageExporter struct {
	instanceName         string
	logger               *zap.Logger
	client               *storage.Client
	cancel               context.CancelFunc
	userAgent            string
	ceSource             string
	ceCompression        Compression
	config               *Config
	tracesMarshaler      ptrace.Marshaler
}

func (*storageExporter) Name() string {
	return name
}

type Encoding int

const (
	OtlpProtoTrace  Encoding = iota
	OtlpProtoMetric          = iota
	OtlpProtoLog             = iota
)

type Compression int

const (
	Uncompressed Compression = iota
	GZip                     = iota
)

type spanStr struct {
    parent string
    id string
    service string
}

type spanWithResource struct {
    span *ptrace.Span
    resource string
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func hash(s string) uint32 {
        h := fnv.New32a()
        h.Write([]byte(s))
        return h.Sum32()
}

func serviceNameToBucketName(serviceName string, suffix string) string {
    // TODO: is this the best way to get it into a format for bucket names?
    // There is probably a more robust way
    bucketID := strings.ReplaceAll(serviceName, ".", "")
    bucketID = strings.ReplaceAll(bucketID, "/", "")
    bucketID = strings.ReplaceAll(bucketID, "google", "")
    bucketID = strings.ReplaceAll(bucketID, "_", "")
    bucketID = strings.ToLower(bucketID)
    return bucketID + suffix
}

func (ex *storageExporter) createBuckets(ctx context.Context) {
    // check this
    var wg sync.WaitGroup
    wg.Add(5)
	go func(ctx context.Context, wg *sync.WaitGroup){
        ex.spanBucketExists(ctx, trace_bucket, false)
        wg.Done()
    }(ctx, &wg)
	go func(ctx context.Context, wg *sync.WaitGroup){
	    ex.spanBucketExists(ctx, "tracehashes", false)
        wg.Done()
    }(ctx, &wg)
	go func(ctx context.Context, wg *sync.WaitGroup){
        ex.spanBucketExists(ctx, "microservices", false)
        wg.Done()
    }(ctx, &wg)
	go func(ctx context.Context, wg *sync.WaitGroup){
        ex.spanBucketExists(ctx, ListBucket, false)
        wg.Done()
    }(ctx, &wg)
	go func(ctx context.Context, wg *sync.WaitGroup){
        ex.spanBucketExists(ctx, HashesByServiceBucket, false)
        wg.Done()
    }(ctx, &wg)

    wg.Wait()
}

func (ex *storageExporter) start(ctx context.Context, _ component.Host) error {
	ctx, ex.cancel = context.WithCancel(ctx)

	if ex.client == nil {
		client, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed creating the gRPC client to Storage: %w", err)
		}

		ex.client = client
	}
    ex.createBuckets(ctx)
    hashNumber = strconv.FormatUint(uint64(hash(os.Getenv("MY_POD_NAME"))), 10)[0:1]
    //seed := int64(hash(os.Getenv("MY_POD_NAME")))
    //ex.logger.Info("seed", zap.String("seed", string(seed)))
    //rand.Seed(seed)
	return nil
}

func (ex *storageExporter) shutdown(context.Context) error {
	if ex.client != nil {
		ex.client.Close()
		ex.client = nil
	}
	return nil
}

func hashTrace(ctx context.Context, spans []spanStr) (map[*spanStr]int, int) {
    // Don't need to do all the computation if you just have one span
    spanToHash := make(map[*spanStr]int)
    if len(spans) == 1 {
        spanToHash[&spans[0]] = int(hash(spans[0].service))*primeNumber
        return spanToHash, spanToHash[&spans[0]]
    }
    // 1. Find root
    var root int
    for i := 0; i< len(spans); i++ {
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
    for i:=0; i<len(spans); i++ {
        for j:=0; j<len(spans); j++ {
            if i != j && spans[j].parent == spans[i].id {
                parentToChild[&spans[i]] = append(parentToChild[&spans[i]], &spans[j])
                childToParent[&spans[j]] = &spans[i]
            }
        }
    }

    // 2b. Fill out level-spanID mappings

    var maxLevel int;
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
        spanToLevel[spanToAssign] = parentLevel+1
        levelToSpans[parentLevel+1] = append(levelToSpans[parentLevel+1], spanToAssign)
        if parentLevel+1 > maxLevel {
            maxLevel = parentLevel + 1
        }
    }

    // 2c.  Use previous two mappings to fill out span ID to hash mappings
    for i:= int(maxLevel); i>=0; i-- {
        //ex.logger.Info("i", zap.Int("i", int(i)))
        // for each level, create hash
        for j:=0; j<len(levelToSpans[i]); j++ {
            span := levelToSpans[i][j]
            spanHash := int(hash(span.service)) + i*primeNumber
            // now add all your children
            for k:=0; k<len(parentToChild[span]); k++ {
                spanHash += spanToHash[parentToChild[span][k]]
            }
            spanToHash[span] = spanHash
        }

    }
    return spanToHash, spanToHash[&spans[root]]
}


func (ex *storageExporter) spanBucketExists(ctx context.Context, serviceName string, isService bool) error {
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
    bkt := ex.client.Bucket(serviceNameToBucketName(serviceName, ex.config.BucketSuffix))
    _, err := bkt.Attrs(ctx)
    if err == storage.ErrBucketNotExist {
        if crErr := bkt.Create(ctx, ex.config.ProjectID, &storageClassAndLocation); crErr != nil {
            var e *googleapi.Error
            if ok := errors.As(crErr, &e); ok {
                if e.Code != 409 { // 409s mean some other thread created the bucket in the meantime;  ignore it
                    return fmt.Errorf("failed creating bucket: %w", crErr)
                } else {
                    ex.logger.Info("got 409")
                    return nil;
                }

            }
        }
    } else if err != nil {
        return fmt.Errorf("failed getting bucket attributes: %w", err)
    }
    return err
}

func (ex *storageExporter) compress(payload []byte) ([]byte, error) {
	switch ex.ceCompression {
	case GZip:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		_, err := writer.Write(payload)
		if err != nil {
			return nil, err
		}
		err = writer.Close()
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return payload, nil
}

// This function is used for debugging only
func (ex *storageExporter) sendDummyData(ctx context.Context, traceID string) error {
    traceBuf := dataBuffer{}
    for i:=0; i<100; i++ {
        traceBuf.logEntry("Number #%d", i)
    }

    trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket, ex.config.BucketSuffix))
    trace_obj := trace_bkt.Object(traceID)
    w_trace := trace_obj.NewWriter(ctx)
    if _, err := w_trace.Write([]byte(traceBuf.buf.Bytes())); err != nil {
        return fmt.Errorf("failed creating the trace object: %s %w", traceID, err)
    }
    if err := w_trace.Close(); err != nil {
        return fmt.Errorf("failed closing the trace object: %s %w", traceID, err)
    }
    return nil
}


// took parts of this code from tail sampling processor here:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/6cb401f1f25d2affcf5a10e737ad1c7e10912206/processor/tailsamplingprocessor/processor.go#L300
func (ex *storageExporter) groupSpansByTraceKey(traces ptrace.Traces) map[pcommon.TraceID][]spanWithResource {
	idToSpans := make(map[pcommon.TraceID][]spanWithResource)
    for i := 0; i<traces.ResourceSpans().Len(); i++ {
        rs := traces.ResourceSpans().At(i)
        if serviceName, ok := rs.Resource().Attributes().Get(conventions.AttributeServiceName); ok {
            ilss := rs.ScopeSpans()
            for j := 0; j < ilss.Len(); j++ {
                spans := ilss.At(j).Spans()
                spansLen := spans.Len()
                for k := spansLen-1; k >= 0; k-- {
                    span := spans.At(k)
                    key := span.TraceID()
                    idToSpans[key] = append(idToSpans[key], spanWithResource {span: &span, resource: serviceName.AsString()})
                }
            }
        }
    }
	return idToSpans
}

func (ex *storageExporter) sendTraceIDsForHashWorker(ctx context.Context, hashToTraceID map[int][]string,
    batch_name string, jobs <-chan int, results chan<- int) {
	bkt := ex.client.Bucket(serviceNameToBucketName("tracehashes", ex.config.BucketSuffix))

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

func (ex *storageExporter) writeMicroserviceToHashMappingWorker(ctx context.Context,
    batch_name string, objectsToWrite map[string][]int, jobs <-chan string, results chan<- int) {

	service_bkt := ex.client.Bucket(serviceNameToBucketName(HashesByServiceBucket, ex.config.BucketSuffix))
	hashesBuf := dataBuffer{}
    for service := range jobs {
        for _, hash := range objectsToWrite[service] {
		    hashesBuf.logEntry(strconv.FormatUint(uint64(hash), 10) + "\n")
        }
        service_obj := service_bkt.Object(service + "/" + batch_name)
        service_writer := service_obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
        if _, err := service_writer.Write(hashesBuf.buf.Bytes()); err != nil {
            println("error in writeMicroserviceToHashMappingWorker: ", err.Error())
        }
        if err := service_writer.Close(); err != nil {
			var e *googleapi.Error
			if ok := errors.As(err, &e); ok {
				if e.Code == 412 { // 409s mean some other thread created the bucket in the meantime;  ignore it
				} else {
                    println("error in closing in writeMicroserviceToHashMappingWorker: ", err.Error())
				}
            }
        }
        results <- 1
    }
}

// Results here is either empty string (this isn't a new hash, someone else got
// there first), or it is the string of the new hash
func (ex *storageExporter) writeHashExemplarsWorker(ctx context.Context, hashToStructure map[int]dataBuffer,
    hashToServices map[int][]string, batch_name string, jobs <-chan int, results chan<- int) {
	list_bkt := ex.client.Bucket(serviceNameToBucketName(ListBucket, ex.config.BucketSuffix))
    for hash := range jobs {
        structure := hashToStructure[hash]
		obj := list_bkt.Object(strconv.FormatUint(uint64(hash), 10))
        w_list := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
        if _, err := w_list.Write(structure.buf.Bytes()); err != nil {
            println(fmt.Errorf("failed creating the list object: %w", err))
            println("error: ", err.Error())
        }
        if err := w_list.Close(); err != nil {
			var e *googleapi.Error
			if ok := errors.As(err, &e); ok {
				if e.Code == 412 { // 409s mean some other thread created the bucket in the meantime;  ignore it
                    results <- 0
				} else {
                    println("error: ", err.Error())
                    results <- hash
				}
            }
        } else {
            results <- hash
        }
    }
}

func (ex *storageExporter) writeHashExemplarsAndHashByMicroservice(ctx context.Context, hashToStructure map[int]dataBuffer,
    hashToServices map[int][]string, batch_name string) int {
    numJobs := len(hashToStructure)
    jobs := make(chan int, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 50

    for w := 1; w <= numWorkers; w++ {
        go ex.writeHashExemplarsWorker(ctx, hashToStructure, hashToServices, batch_name, jobs, results)
    }

    for hash, _ := range hashToStructure {
        jobs <- hash
    }
    close(jobs)

    totalNew := 0
    objectsToWrite := make(map[string][]int)
    for a := 1; a <= numJobs; a++ {
        result := <-results
        if result != 0 {
            totalNew += 1
            for _, service := range hashToServices[result] {
                objectsToWrite[service] = append(objectsToWrite[service], result)
            }
        }
    }

    // Now create all the hash objects
    hashNumJobs := len(objectsToWrite)
    hashJobs := make(chan string, hashNumJobs)
    hashResults := make(chan int, hashNumJobs)

    numWorkers = 50

    for w := 1; w <= numWorkers; w++ {
        go ex.writeMicroserviceToHashMappingWorker(ctx, batch_name, objectsToWrite, hashJobs, hashResults)
    }
    for service, _ := range objectsToWrite {
        hashJobs <- service
    }
    close(hashJobs)
    for a := 1; a <= hashNumJobs; a++ {
        <-hashResults
    }
    return totalNew
}

func (ex *storageExporter) sendHashToTraceIDMapping(ctx context.Context, hashToTraceID map[int][]string, batch_name string) {

    numJobs := len(hashToTraceID)
    jobs := make(chan int, numJobs)
    results := make(chan int, numJobs)

    numWorkers := 30

    for w := 1; w <= numWorkers; w++ {
        go ex.sendTraceIDsForHashWorker(ctx, hashToTraceID, batch_name, jobs, results)
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

// Stores 2 things in GCS:
// 1. Trace ID to hash and struct
// 2. Hash to trace ID
func (ex *storageExporter) storeHashAndStruct(traceIDToSpans map[pcommon.TraceID][]spanWithResource, objectName string) error {
    // 1. Collect the trace structures in traceStructBuf, and a map of hashes to traceIDs
    ctx := context.Background()
    traceStructBuf := dataBuffer{}
	hashToTraceID := make(map[int][]string)
	hashToStructure := make(map[int]dataBuffer)
    hashToServices := make(map[int][]string)
    for traceID, spans := range traceIDToSpans {
        var sp []spanStr
        traceStructBuf.logEntry("Trace ID: %s:", traceID.String())
        for i := 0; i< len(spans); i++ {
            parent := spans[i].span.ParentSpanID().String()
            spanID := spans[i].span.SpanID().String()
            resource := spans[i].resource
            sp = append(sp, spanStr{
               parent: parent,
               id: spanID,
               service: resource})
        }
        hashmap, hash := hashTrace(ctx, sp)
		// If the hash to structure has not yet been filled, fill it.
		hashFilled := true
		structBuf := dataBuffer{}
		if _, ok := hashToStructure[hash]; !ok {
			hashFilled = false
			structBuf.logEntry("Trace ID: %s:", traceID.String())
		}
		for i := 0; i < len(sp); i++ {
			traceStructBuf.logEntry("%s:%s:%s:%s", sp[i].parent, sp[i].id, sp[i].service,
				strconv.FormatUint(uint64(hashmap[&sp[i]]), 10))
			if !hashFilled {
				structBuf.logEntry("%s:%s:%s:%s", sp[i].parent, sp[i].id, sp[i].service,
					strconv.FormatUint(uint64(hashmap[&sp[i]]), 10))
			}

		}
		hashToTraceID[hash] = append(hashToTraceID[hash], traceID.String())
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
	trace_bkt := ex.client.Bucket(serviceNameToBucketName(trace_bucket, ex.config.BucketSuffix))

	trace_obj := trace_bkt.Object(objectName)
	w_trace := trace_obj.NewWriter(ctx)
	if _, err := w_trace.Write([]byte(traceStructBuf.buf.Bytes())); err != nil {
		return fmt.Errorf("failed creating the trace object: %w", err)
	}
	if err := w_trace.Close(); err != nil {
		return fmt.Errorf("failed closing the trace object %w", err)
	}
    //fmt.Println("time to send trace struct buffer: ", time.Since(computed_time))

    //before_hash_mapping_time := time.Now()

	// 3. Put the hash to trace ID mapping in storage
    ex.sendHashToTraceIDMapping(ctx, hashToTraceID, objectName)
    //fmt.Println("time to send hash to trace ID mapping: ", time.Since(before_hash_mapping_time))

    last_time := time.Now()
    _ = ex.writeHashExemplarsAndHashByMicroservice(ctx, hashToStructure, hashToServices, objectName)
    fmt.Println("time to write hash exemplars: ", time.Since(last_time))

	return nil
}

// A helper function that stores spans according to their resource.
func (ex *storageExporter) storeSpans(traces ptrace.Traces, objectName string) error {
    ctx := context.Background()
    rss := traces.ResourceSpans()
    for i := 0; i< rss.Len(); i++ {
        // Here we marshal by resourcespan;  because the groupbyattr processor is always used, we can be confident that
        // they have been grouped by resource already.
        // 1. Marshal the spans from the same resource into a buffer
		if sn, ok := rss.At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
            oneResourceSpans := ptrace.NewTraces()
            rss.At(i).CopyTo(oneResourceSpans.ResourceSpans().AppendEmpty())
            buffer, err := ex.tracesMarshaler.MarshalTraces(oneResourceSpans)
            if err != nil {
                ex.logger.Info("could not marshal traces ", zap.Error(err))
                return err
            }

            // 2. Determine the bucket of the new object, and make sure it's a bucket that exists
            bucketName := serviceNameToBucketName("microservices", ex.config.BucketSuffix)
            bkt := ex.client.Bucket(bucketName)
            // 3. Send the data under that bucket/object name to storage
            obj := bkt.Object(sn.AsString() + ex.config.BucketSuffix + "/" + objectName)
            writer := obj.NewWriter(ctx)
            if _, err := writer.Write(buffer); err != nil {
                return fmt.Errorf("failed creating the span object: %w", err)
            }
            if err := writer.Close(); err != nil {
                return fmt.Errorf("failed closing the span object: %w", err)
            }
        } else {
            ex.logger.Info("didn't get service name")
        }
    }
    return nil
}


// This is the main function of the exporter.  It is called by consumers
// to process trace data and send it to GCS.
func (ex *storageExporter) consumeTraces(ctx context.Context, traces ptrace.Traces) error {
    // once you have a batch, there are two things you must do with it:

    traceIDToSpans := ex.groupSpansByTraceKey(traces)

    // 1. Find time span
    minTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
    maxTime := time.Date(2020, 2, 11, 20, 26, 12, 321, time.UTC) // dummy value, will be overwritten
    first_iteration := true
    for _, spans := range traceIDToSpans {
        for i := 0; i< len(spans); i++ {
            if first_iteration || spans[i].span.StartTimestamp().AsTime().Before(minTime) {
                minTime = spans[i].span.StartTimestamp().AsTime()
            }
            if first_iteration || spans[i].span.EndTimestamp().AsTime().After(maxTime) {
                maxTime = spans[i].span.StartTimestamp().AsTime()
            }
            first_iteration = false
        }
    }
    minTimeStr := strconv.FormatUint(uint64(minTime.Unix()), 10)
    maxTimeStr := strconv.FormatUint(uint64(maxTime.Unix()), 10)
    random, _ := rand.Int(rand.Reader, big.NewInt(100000000))
    int_hash := strconv.FormatUint(uint64(random.Int64()), 10)
    if len(int_hash) == 1 {
        int_hash = "0"+int_hash
    }
    numDigits, err := strconv.Atoi(ex.config.NumOfDigitsForRandomHash)
    if err != nil {
        ex.logger.Error("error converting %s", zap.NamedError("error", err))
        return err
    }
    if len(int_hash) > numDigits {
        int_hash = int_hash[0:numDigits-1]
    }
    objectName := int_hash + hashNumber + "-" + minTimeStr + "-" + maxTimeStr
    // 1. push spans to storage
    ret := ex.storeSpans(traces, objectName)
    if ret != nil {
        ex.logger.Error("error storing spans %s", zap.NamedError("error", ret))
        return ret
    }
    // 2. push trace structure as well as the hash of the structure to storage
    // 2a. Create a mapping from trace ID to each of the spans in the trace
    ret =  ex.storeHashAndStruct(traceIDToSpans, objectName)
    if ret != nil {
        ex.logger.Error("error storing trace structure and hash %s", zap.NamedError("error", ret))
    }
    return nil
}

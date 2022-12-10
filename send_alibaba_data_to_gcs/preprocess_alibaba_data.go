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
	mathrand "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"

	storage "cloud.google.com/go/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.5.0"
	"google.golang.org/api/googleapi"
)

const (
	ProjectName             = "cost-project-1"
	TraceBucket             = "dyntraces"
	PrimeNumber             = 97
	BucketSuffix            = "-alibaba-and-forty-thieves"
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
	newSpan.timestamp = newSpan.timestamp + 1670427276 // We want realistic timestamps, so just adding time as of Dec 7th to get offsets in the 12 hour Alibaba period
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
			var e *googleapi.Error
			if ok := errors.As(crErr, &e); ok {
				if e.Code != 409 { // 409s mean some other thread created the bucket in the meantime;  ignore it
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
	return err
}

func isCyclic2(aliBabaSpans []AliBabaSpan, v int, upstreamMap map[string][]int, visited map[int]bool, recStack []bool) bool {

    if visited[v] == false {
        println("visiting: ", v)

        // Do not visit or recstack if this is a self loop
        if aliBabaSpans[v].downstream_microservice != aliBabaSpans[v].upstream_microservice {
            return false;
        }
        visited[v] = true;
        recStack[v] = true;

        for _, child := range upstreamMap[aliBabaSpans[v].downstream_microservice] {
            if visited[child] == false && isCyclic2(aliBabaSpans, child, upstreamMap, visited, recStack) {
                println("returning true on ", child)
                return true
            } else if recStack[child] {
                println("returning true on ", child)
                return true
            }
        }
    }
    recStack[v] = false;
    return false;
}

func isCyclic(aliBabaSpans []AliBabaSpan, root_ind int, upstreamMap map[string][]int) bool {
    println("guess what it's the upstream mpa")
    for i, j := range upstreamMap {
        println("upstream map key: ", i, "val: ")
        for _, k := range j {
            print(k)
        }
        println("")
    }


	stack := make([]int, 0)
	stack = append(stack, root_ind)
	visited := make(map[int]bool)

    for {
        if len(stack) < 1 {
            break
        }
        println("visited: ")
        for i,_ := range visited {
            print(i, "  ")
        }
        println("")

        println("stack: ")
        for i := 0; i < len(stack); i++ {
            print(stack[i], "  ")
        }
        println("")

        top := stack[len(stack)-1]
        visited[top] = true
        stack = stack[:len(stack)-1]
        println("top is ", top)
        if aliBabaSpans[top].downstream_microservice == aliBabaSpans[top].upstream_microservice {
            println("continuing bc it is the same")
            continue
        }
        println("stack after removal: ")
        for i := 0; i < len(stack); i++ {
            print(stack[i], "  ")
        }
        println("")

        println("alibabaspans top is ", aliBabaSpans[top].downstream_microservice)

        for _, child := range upstreamMap[aliBabaSpans[top].downstream_microservice] {
            println("my child is ", child)
            if visited[child] == false {
                stack = append(stack, child)
            }
            if visited[child] == true && aliBabaSpans[child].upstream_microservice != MissingData {
                println("index ", child, " already was visited")
                return true
            }
        }
    }
    return false
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
		batch.Resource().Attributes().PutStr("service.name", aliBabaSpan.downstream_microservice) // what if dm is missing ?
		batch.Resource().Attributes().PutStr("upstream.name", aliBabaSpan.upstream_microservice)  // what if dm is missing ?
        println("upstream is ", aliBabaSpan.upstream_microservice)
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
            println("sn: ", sn.AsString())
            for _, k := range upstreamMap[sn.AsString()] {
                print(k, " ")
            }
            println()
		}

		if rpc_id, ok := traces.ResourceSpans().At(i).Resource().Attributes().Get("rpc.id"); ok {
			if rpc_id.AsString() == "0.1" {
				root_span_index = i
			} else if rpc_id.AsString() == "0.1.1" && root_span_index == -1 {
				root_span_index = i
			}
		}
	}
    println("it's after creating the upstream mpa")
    for i, j := range upstreamMap {
        println("upstream map key: ", i)
        print("val: ")
        for _, k := range j {
            print(k)
        }
        println("")
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
    if (isCyclic2(aliBabaSpans, root_span_index, upstreamMap, cyclic_visited, recStack)) {
        // :(
        println("unfortuantely i am cyclic")
        return TimeWithTrace{}
    }

	for {
		if len(queue) < 1 {
			break
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

		// log.Println("Adding: ")
		for _, ele := range nextLevel {
			// log.Print(ele, " ")
			traces.ResourceSpans().At(ele).ScopeSpans().At(0).Spans().At(0).SetParentSpanID(span_id)
			queue = append(queue, ele)
		}
		// log.Println(".")
	}

	// Unreachibility thin'
	for ind := 0; ind < traces.ResourceSpans().Len(); ind++ {
		if _, ok := visited[ind]; !ok {
            println("trace id of unreachable trace is : ", aliBabaSpans[root_span_index].trace_id)
            /*
		    if sn, ok := traces.ResourceSpans().At(ind).Resource().Attributes().Get("rpc.id"); ok {
                println("unreachable has rpc id of ", sn.AsString())
            }
            */
			return TimeWithTrace{}
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

func sendBatchSpansToStorage(traces []TimeWithTrace, batch_name string, client *storage.Client) error {
	ctx := context.Background()
	resourceNameToSpans := make(map[string]ptrace.Traces)
	for time_with_trace := range traces {
		span := traces[time_with_trace].trace
		for i := 0; i < span.ResourceSpans().Len(); i++ {
			if sn, ok := span.ResourceSpans().At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
                //println("considering span of resource ", sn.AsString())
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
	tracesMarshaler := &ptrace.ProtoMarshaler{}
	for resource, spans := range resourceNameToSpans {
        //println("sending resource ", resource)
		bucketName := serviceNameToBucketName(resource, BucketSuffix)
		bkt := client.Bucket(bucketName)

		// Check if bucket exists or not, create one if needed
        spanBucketExists(ctx, resource, true, client)
		buffer, err := tracesMarshaler.MarshalTraces(spans)
		if err != nil {
			print("could not marshal traces")
			return err
		}
		obj := bkt.Object(batch_name)
		ctx := context.Background()
		writer := obj.NewWriter(ctx)
		if _, err := writer.Write(buffer); err != nil {
			return fmt.Errorf("failed creating the span object: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed closing the span object: %w", err)
		}
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
		//ex.logger.Info("i", zap.Int("i", int(i)))
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

func computeHashesAndTraceStructToStorage(traces []TimeWithTrace, batch_name string, client *storage.Client) error {
	// 1. Collect the trace structures in traceStructBuf, and a map of hashes to traceIDs
	ctx := context.Background()
	traceStructBuf := dataBuffer{}
	hashToTraceID := make(map[int][]string)
	for _, trace := range traces {
		traceID := trace.trace.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()
		var sp []spanStr
		traceStructBuf.logEntry("Trace ID: %s:", traceID.HexString())
		for i := 0; i < trace.trace.ResourceSpans().Len(); i++ {
			span := trace.trace.ResourceSpans().At(i).ScopeSpans().At(0).Spans().At(0)
			parent := span.ParentSpanID().HexString()
			spanID := span.SpanID().HexString()
			if sn, ok := trace.trace.ResourceSpans().At(i).Resource().Attributes().Get(conventions.AttributeServiceName); ok {
				sp = append(sp, spanStr{
					parent:  parent,
					id:      spanID,
					service: sn.AsString()})

			}
		}
		hashmap, hash := hashTrace(ctx, sp)
		for i := 0; i < len(sp); i++ {
			traceStructBuf.logEntry("%s:%s:%s:%s", sp[i].parent, sp[i].id, sp[i].service,
				strconv.FormatUint(uint64(hashmap[&sp[i]]), 10))
		}
		hashToTraceID[hash] = append(hashToTraceID[hash], traceID.HexString())
	}

	// 2. Put the trace structure buffer in storage
	trace_bkt := client.Bucket(serviceNameToBucketName(TraceBucket, BucketSuffix))
	spanBucketExists(ctx, TraceBucket, false, client)

	trace_obj := trace_bkt.Object(batch_name)
	w_trace := trace_obj.NewWriter(ctx)
	if _, err := w_trace.Write([]byte(traceStructBuf.buf.Bytes())); err != nil {
		return fmt.Errorf("failed creating the trace object: %w", err)
	}
	if err := w_trace.Close(); err != nil {
		return fmt.Errorf("failed closing the trace object %w", err)
	}
	// 3. Put the hash to trace ID mapping in storage
	bkt := client.Bucket(serviceNameToBucketName("tracehashes", BucketSuffix))
	spanBucketExists(ctx, "tracehashes", false, client)
	for hash, traces := range hashToTraceID {
		traceIDs := dataBuffer{}
		for i := 0; i < len(traces); i++ {
			traceIDs.logEntry("%s", traces[i])
		}
		obj := bkt.Object(strconv.FormatUint(uint64(hash), 10) + "/" + batch_name)
		w := obj.NewWriter(ctx)
		if _, err := w.Write(traceIDs.buf.Bytes()); err != nil {
			return fmt.Errorf("failed creating the object: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("failed closing the hash object in bucket %s: %w", strconv.FormatUint(uint64(hash), 10)+"/"+batch_name, err)
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		println("usage: ./preprocess_alibaba_data filename")
		os.Exit(0)
	}

	filename := os.Args[1]

	// determine if name mapping file exists
	microservice_hash_to_name := importNameMapping()
	traceIDToAliBabaSpans := importAliBabaData(filename, 1, microservice_hash_to_name)
	pdataTraces := make([]TimeWithTrace, 0)
	empty := TimeWithTrace{}
    totalTraces := 0
    exemptedTraces := 0
	for _, aliBabaSpans := range traceIDToAliBabaSpans {
		// We need to create pdata spans
		timeAndpdataSpans := makePData(aliBabaSpans)
        totalTraces += 1
		if timeAndpdataSpans != empty {
			pdataTraces = append(pdataTraces, timeAndpdataSpans)
		} else {
            exemptedTraces += 1
        }
	}

    println("total traces: ", totalTraces)
    println("exempted traces: ", exemptedTraces)
	println("organizing spans by time")
	println("pdata traces at first size is ", len(pdataTraces))

	// Then organize the spans by time, and batch them.
	sort.Slice(pdataTraces, func(i, j int) bool {
		return pdataTraces[i].timestamp < pdataTraces[j].timestamp
	})

	// Now, we batch.
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		print("could not create gcs client")
		os.Exit(0)
	}
	_ = client

	j := 0
	for j < len(pdataTraces) {
		println("j is ", j)
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
		_ = batch_name
		sendBatchSpansToStorage(pdataTraces[start:end], batch_name, client)
		computeHashesAndTraceStructToStorage(pdataTraces[start:end], batch_name, client)
		j += BatchSize
	}
	println("done with everything")
}

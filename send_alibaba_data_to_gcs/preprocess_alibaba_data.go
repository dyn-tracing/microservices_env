package main

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"sort"
	"strconv"

	storage "cloud.google.com/go/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.5.0"
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

type TimeWithTrace struct {
	timestamp int
	trace     ptrace.Traces
}

func createAliBabaSpan(row []string) AliBabaSpan {
	var newSpan AliBabaSpan
	newSpan.timestamp, _ = strconv.Atoi(row[0])
	newSpan.trace_id = row[1]
	newSpan.rpc_id = row[2]
	newSpan.upstream_microservice = row[3]
	newSpan.rpc_type = row[4]
	newSpan.ali_interface = row[5]
	newSpan.downstream_microservice = row[6]
	newSpan.response_time, _ = strconv.Atoi(row[7])
	return newSpan
}

func importAliBabaData(filename string, filenum int) map[string][]AliBabaSpan {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	mapping := make(map[string][]AliBabaSpan)

	csvReader := csv.NewReader(f)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		newSpan := createAliBabaSpan(rec)
		mapping[newSpan.trace_id] = append(mapping[newSpan.trace_id], newSpan)
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

func makePData(aliBabaSpans []AliBabaSpan) TimeWithTrace {
	root_span_index := -1

	for ind, aliBabaSpan := range aliBabaSpans {
		if aliBabaSpan.rpc_id == "0.1" {
			root_span_index = ind
		}
	}

	if root_span_index == -1 {
		for ind, aliBabaSpan := range aliBabaSpans {
			if aliBabaSpan.rpc_id == "0.1.1" {
				root_span_index = ind
			}
		}
	}

	if root_span_index == -1 {
		return TimeWithTrace{}
	}

	traces := ptrace.NewTraces()
	earliest_time := aliBabaSpans[0].timestamp
	upstreamMap := make(map[string][]int)

	for ind, aliBabaSpan := range aliBabaSpans {
		upstreamMap[aliBabaSpan.upstream_microservice] = append(upstreamMap[aliBabaSpan.upstream_microservice], ind)

		if aliBabaSpan.timestamp < earliest_time {
			earliest_time = aliBabaSpan.timestamp
		}

		batch := traces.ResourceSpans().AppendEmpty()
		batch.Resource().Attributes().PutStr("service.name", aliBabaSpan.upstream_microservice)
		ils := batch.ScopeSpans().AppendEmpty()
		span := ils.Spans().AppendEmpty()

		trace_id_bytes, err := hex.DecodeString(aliBabaSpan.trace_id)
		trace_id := pcommon.TraceID(bytesTo16Bytes(trace_id_bytes))
		span.SetTraceID(trace_id)
		_ = err
	}

	queue := make([]int, 0)
	queue = append(queue, root_span_index)
	visited := make(map[int]bool)

	for {
		if len(queue) < 1 {
			break
		}
		top := queue[0]

		// Checking for cyclicty in traces
		if _, ok := visited[top]; ok {
			return TimeWithTrace{}
		} else {
			visited[top] = true
		}

		queue = queue[1:]
		dm := aliBabaSpans[top].downstream_microservice
		nextLevel := upstreamMap[dm]
		_ = nextLevel

		raw_span_id := make([]byte, 16)
		rand.Read(raw_span_id)
		span_id := pcommon.SpanID(bytesTo8Bytes(raw_span_id))
		traces.ResourceSpans().At(top).ScopeSpans().At(0).Spans().At(0).SetSpanID(span_id)
		traces.ResourceSpans().At(top).ScopeSpans().At(0).Spans().At(0).SetParentSpanID(pcommon.SpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 0}))

		for _, ele := range nextLevel {
			traces.ResourceSpans().At(ele).ScopeSpans().At(0).Spans().At(0).SetParentSpanID(span_id)
		}
	}

	// Unreachibility thin'
	for ind := range aliBabaSpans {
		if _, ok := visited[ind]; !ok {
			return TimeWithTrace{}
		}
	}

	return TimeWithTrace{earliest_time, traces}
}

func serviceNameToBucketName(service string, suffix string) string {
	return service + "-" + suffix
}

func sendBatchSpansToStorage(traces []TimeWithTrace, batch_name string, client *storage.Client, bucket_suffix string) error {
	ctx := context.Background()
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
			}
		}
	}

	// 3. Send each resource's spans to storage
	tracesMarshaler := &ptrace.ProtoMarshaler{}
	for resource, spans := range resourceNameToSpans {
		bucketName := serviceNameToBucketName(resource, bucket_suffix)
		bkt := client.Bucket(bucketName)

		// Check if bucket exists or not, create one if needed
		_, err := bkt.Attrs(ctx)
		if err == storage.ErrBucketNotExist {
			err = bkt.Create(ctx, "dyn-tracing", nil)
			if err != nil {
				print("Could not create bucket ", bucketName)
			}
		}

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

func computeHashesAndTraceStructToStorage(traces []TimeWithTrace, batch_name string, client *storage.Client) {

}

func main() {
	if len(os.Args) != 2 {
		print("usage: ./preprocess_alibaba_data filename")
		os.Exit(0)
	}

	filename := os.Args[1]
	traceIDToAliBabaSpans := importAliBabaData(filename, 1)
	pdataTraces := make([]TimeWithTrace, 0)
	for _, aliBabaSpans := range traceIDToAliBabaSpans {
		// We need to create pdata spans
		timeAndpdataSpans := makePData(aliBabaSpans)
		pdataTraces = append(pdataTraces, timeAndpdataSpans)
	}
	// TODO: Then organize the spans by time, and batch them.
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

	j := 0
	for j < len(pdataTraces) {
		start := j
		end := start + 1000
		if end >= len(pdataTraces) {
			end = len(pdataTraces) - 1
		}
		// Name of this batch is...
		random, _ := rand.Int(rand.Reader, big.NewInt(100000000))
		int_hash := strconv.FormatUint(uint64(random.Int64()), 10)
		if len(int_hash) == 1 {
			int_hash = "0" + int_hash
		}
		batch_name := int_hash[0:2] +
			string(pdataTraces[start].timestamp) + string(pdataTraces[end].timestamp)
		sendBatchSpansToStorage(pdataTraces[start:end], batch_name, client, "-snicket-alibaba")
		computeHashesAndTraceStructToStorage(pdataTraces[start:end], batch_name, client)
	}
}

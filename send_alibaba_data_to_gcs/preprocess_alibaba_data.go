package main

import (
	"encoding/csv"
	"encoding/hex"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
		if _, ok := mapping[newSpan.trace_id]; ok {
			mapping[newSpan.trace_id] = append(mapping[newSpan.trace_id], newSpan)
		} else {
			mapping[newSpan.trace_id] = []AliBabaSpan{newSpan}
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
		if _, ok := upstreamMap[aliBabaSpan.upstream_microservice]; ok {
			upstreamMap[aliBabaSpan.upstream_microservice] = append(upstreamMap[aliBabaSpan.upstream_microservice], ind)
		} else {
			upstreamMap[aliBabaSpan.upstream_microservice] = []int{ind}
		}

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

func main() {
	if len(os.Args) != 2 {
		print("usage: ./preprocess_alibaba_data filename")
		os.Exit(0)
	}
	filename := os.Args[1]
	traceIDToAliBabaSpans := importAliBabaData(filename, 1)
	for trace_id, aliBabaSpans := range traceIDToAliBabaSpans {
		// We need to create pdata spans
		pdataSpans := makePData(aliBabaSpans)
		_ = pdataSpans
		_ = trace_id
	}
	// TODO: Then organize the spans by time, and batch them.

	// TODO: Compute hashes

	// TODO: Send to storage
}

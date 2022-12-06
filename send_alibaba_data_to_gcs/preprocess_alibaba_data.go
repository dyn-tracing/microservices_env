package main

import (
    "encoding/csv"
    "os"
    "io"
    "log"
    "strconv"
    "go.opentelemetry.io/collector/pdata/ptrace"
)

type AliBabaSpan struct {
    timestamp int
    trace_id string
    rpc_id string
    upstream_microservice string
    rpc_type string
    ali_interface string
    downstream_microservice string
    response_time int
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

func makePData(aliBabaSpans []AliBabaSpan) ptrace.Traces {
    // Make a pdata of the data.
    traces := ptrace.NewTraces()
    for _, aliBabaSpan := range aliBabaSpans {
        batch := traces.ResourceSpans.AppendEmpty()
        rs.Resource().Attributes().PutStr("service.name", aliBabaSpan.upstream_microservice)
        ils := batch.ScopeSpans().AppendEmpty()
        span := ils.Spans().AppendEmpty()
        span.SetTraceID(hex.DecodeString(aliBabaSpan.trace_id))
    }

    // TODO: Now identify the root span
    // TODO: Follow the root span down, creating span IDs and identifying parent span IDs.
    return traces
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
    }
    // TODO: Then organize the spans by time, and batch them.

    // TODO: Compute hashes

    // TODO: Send to storage
    _ = aliBabaData
}

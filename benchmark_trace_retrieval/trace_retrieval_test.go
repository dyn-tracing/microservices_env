package main

import (
    "context"
    "log"
    "cloud.google.com/go/storage"
    "testing"
)

var result string

func benchmarkTraceRetrieval(traceID string, b *testing.B) {
    var s string
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
            log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        s, _ = getTrace(traceID, client)
    }
    result = s
}

func BenchmarkBigTrace(b *testing.B) {
    benchmarkTraceRetrieval("82b29a11332a18878a7d5664b583d983", b)
}

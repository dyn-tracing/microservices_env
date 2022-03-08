package main

import (
    "context"
    "log"
    "cloud.google.com/go/storage"
    "testing"
    "bytes"
)

var result string
var result2 []byte

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

func benchmarkTraceRetrievalParallel(traceID string, b *testing.B) {
    var s string
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
            log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        s, _ = getTraceParallelized(traceID, client)
    }
    result = s
}

func BenchmarkBigTrace(b *testing.B) {
    benchmarkTraceRetrieval("82b29a11332a18878a7d5664b583d983", b)
}
func BenchmarkBigTraceParallel(b *testing.B) {
    benchmarkTraceRetrievalParallel("82b29a11332a18878a7d5664b583d983", b)
}

func BenchmarkFourSpanTrace(b *testing.B) {
    benchmarkTraceRetrieval("52b22dcfef3d16ac0bf6634f9ba61d5f", b)
}

func BenchmarkFourSpanTraceParallel(b *testing.B) {
    benchmarkTraceRetrievalParallel("52b22dcfef3d16ac0bf6634f9ba61d5f", b)
}
func benchmarkFileRetrieval(filename string, b *testing.B) {
    var s []byte
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
            log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()
    b.ResetTimer()
    var buf bytes.Buffer
    for i := 0; i < b.N; i++ {
        s, _ = downloadFileIntoMemory(&buf, "benchmark-gcs-snicket", filename, client)
    }
    result2 = s
}

func BenchmarkTwoBytes(b *testing.B) {
    benchmarkFileRetrieval("twobytes.txt", b)
}


func BenchmarkHundredBytes(b *testing.B) {
    benchmarkFileRetrieval("hundredbytes.txt", b)
}

func BenchmarkThousandBytes(b *testing.B) {
    benchmarkFileRetrieval("thousandbytes.txt", b)
}

func BenchmarkTenThousandBytes(b *testing.B) {
    benchmarkFileRetrieval("tenthousandbytes.txt", b)
}

func BenchmarkHundredThousandBytes(b *testing.B) {
    benchmarkFileRetrieval("hundredthousandbytes.txt", b)
}

func BenchmarkMegabyte(b *testing.B) {
    benchmarkFileRetrieval("megabyte.txt", b)
}

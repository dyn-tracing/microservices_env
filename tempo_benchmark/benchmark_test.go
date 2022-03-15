package main

import (
	// "fmt"
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"log"
	"testing"
)

// const BigTrace = "6009004d2e8ca99b64a9a4e1924e4de3" //31 spans tempo 49KB zstd snappy
// const BigTrace = "a5d1800f3aa97ad534d54a0263c4645c" //31 spans jaeger
// const SmallTrace = "0a9fc375450a9bbb5fc545ecbf0fda4a" //4 spans tempo 8KB zstd snappy
// const SmallTrace = "61aee0daf0a7e29ac35bdc243c395f93" //4 spans jaeger
// const TinyTrace = "9767ed368bf2053d8ac8c360e799d3f2"  //1 span tempo 4KB zstd snappy
// const TinyTrace = "7cd39c7f87b4c84fbea56d3a7c049d05" //1 span jaeger
const BigTrace = "3a2cc78108bb208e78696e07767a7660"   // 31 spans tempo none
const SmallTrace = "fc215a13ba225fda23a1479cd8b3fd0b" // 4 spans tempo none
const TinyTrace = "11ae3837b779586559764e20cbbfb4ef"  // 1 span tempo none
const TwoBytes = "twobytes.txt"
const HundredBytes = "hundredbytes.txt"
const ThousandBytes = "thousandbytes.txt"
const TenThousandBytes = "tenthousandbytes.txt"
const HundredThousandBytes = "hundredthousandbytes.txt"
const MegaBytes = "megabyte.txt"
const BigTraceBytes = "bigtrace-49KB"
const SmallTraceBytes = "smalltrace-8KB"
const TinyTraceBytes = "tinytrace-4KB"

func benchmarkGetTrace(b *testing.B, traceId string) {
	for i := 0; i < b.N; i++ {
		err, _ := getTrace(traceId)
		if err != nil {
			b.Errorf(err.Error())
		}
	}
}

func benchmarkGetFile(filename string, b *testing.B) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	b.ResetTimer()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		downloadFileIntoMemory(&buf, "benchmark-gcs-snicket", filename, client)
		// log.Println(".." + fmt.Sprint(len(d)))
	}
}

func BenchmarkGetBigTrace(b *testing.B) {
	benchmarkGetTrace(b, BigTrace)
}

func BenchmarkGetSmallTrace(b *testing.B) {
	benchmarkGetTrace(b, SmallTrace)
}

func BenchmarkGetTinyTrace(b *testing.B) {
	benchmarkGetTrace(b, TinyTrace)
}

func BenchmarkGetTwoBytes(b *testing.B) {
	benchmarkGetFile(TwoBytes, b)
}

func BenchmarkGetHundredBytes(b *testing.B) {
	benchmarkGetFile(HundredBytes, b)
}

func BenchmarkGetThousandBytes(b *testing.B) {
	benchmarkGetFile(ThousandBytes, b)
}

func BenchmarkGetTenThousandBytes(b *testing.B) {
	benchmarkGetFile(TenThousandBytes, b)
}

func BenchmarkGetHundredThousandBytes(b *testing.B) {
	benchmarkGetFile(HundredThousandBytes, b)
}

func BenchmarkGetMegaBytes(b *testing.B) {
	benchmarkGetFile(MegaBytes, b)
}

func BenchmarkGetBigTraceBytes(b *testing.B) {
	benchmarkGetFile(BigTraceBytes, b)
}

func BenchmarkGetSmallTraceBytes(b *testing.B) {
	benchmarkGetFile(SmallTraceBytes, b)
}

func BenchmarkGetTinyTraceBytes(b *testing.B) {
	benchmarkGetFile(TinyTraceBytes, b)
}

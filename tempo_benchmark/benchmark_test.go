package main

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"log"
	"os"
	"testing"
)

const WriteBucketName = "gcs_write_bench"

// const BigTrace = "6009004d2e8ca99b64a9a4e1924e4de3" //31 spans tempo 49KB zstd snappy
// const BigTrace = "a5d1800f3aa97ad534d54a0263c4645c" //31 spans jaeger
// const SmallTrace = "0a9fc375450a9bbb5fc545ecbf0fda4a" //4 spans tempo 8KB zstd snappy
// const SmallTrace = "61aee0daf0a7e29ac35bdc243c395f93" //4 spans jaeger
// const TinyTrace = "9767ed368bf2053d8ac8c360e799d3f2"  //1 span tempo 4KB zstd snappy
// const TinyTrace = "7cd39c7f87b4c84fbea56d3a7c049d05" //1 span jaeger
const BigTrace = "2472d3e83992e79d7175440200670290"   // 30 spans tempo none
const SmallTrace = "20915f0819ea88f9f904f675a8403d32" // 4 spans tempo none
const TinyTrace = "26339b77c78609ed92c82a36a47ecb3f"  // 1 span tempo none
const TwoBytes = "twobytes.txt"
const HundredBytes = "hundredbytes.txt"
const ThousandBytes = "thousandbytes.txt"
const TenThousandBytes = "tenthousandbytes.txt"
const HundredThousandBytes = "hundredthousandbytes.txt"
const MegaBytes = "megabyte.txt"
const BigTraceBytes = "bigtrace-49KB"
const SmallTraceBytes = "smalltrace-8KB"
const TinyTraceBytes = "tinytrace-4KB"

func benchmarkGraphQuery(parent string, child string, overallLatency string, duration int64, b *testing.B) {
	for i := 0; i < b.N; i++ {
		ts, err := getTracesWhereXCallsY(parent, child, overallLatency, duration)
		if err != nil {
			b.Errorf(err.Error())
		}
		_ = ts
		// log.Println(ts)
	}
}

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

func benchmarkWriteFile(objName string, b *testing.B) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	bkt := client.Bucket(WriteBucketName)
	obj := bkt.Object(objName + "." + randStr(10))

	w := obj.NewWriter(ctx)
	defer w.Close()

	dataBytes, err := os.ReadFile("./data/" + objName)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	data := string(dataBytes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := uploadDataToGCS(w, data)
		if err != nil {
			log.Fatalf("Failed to write object: %v", err)
		}
	}
}

// func BenchmarkGetBigTrace(b *testing.B) {
// 	benchmarkGetTrace(b, BigTrace)
// }

// func BenchmarkGetSmallTrace(b *testing.B) {
// 	benchmarkGetTrace(b, SmallTrace)
// }

// func BenchmarkGetTinyTrace(b *testing.B) {
// 	benchmarkGetTrace(b, TinyTrace)
// }

// func BenchmarkGetTwoBytes(b *testing.B) {
// 	benchmarkGetFile(TwoBytes, b)
// }

// func BenchmarkGetHundredBytes(b *testing.B) {
// 	benchmarkGetFile(HundredBytes, b)
// }

// func BenchmarkGetThousandBytes(b *testing.B) {
// 	benchmarkGetFile(ThousandBytes, b)
// }

// func BenchmarkGetTenThousandBytes(b *testing.B) {
// 	benchmarkGetFile(TenThousandBytes, b)
// }

// func BenchmarkGetHundredThousandBytes(b *testing.B) {
// 	benchmarkGetFile(HundredThousandBytes, b)
// }

// func BenchmarkGetMegaBytes(b *testing.B) {
// 	benchmarkGetFile(MegaBytes, b)
// }

// func BenchmarkGetBigTraceBytes(b *testing.B) {
// 	benchmarkGetFile(BigTraceBytes, b)
// }

// func BenchmarkGetSmallTraceBytes(b *testing.B) {
// 	benchmarkGetFile(SmallTraceBytes, b)
// }

// func BenchmarkGetTinyTraceBytes(b *testing.B) {
// 	benchmarkGetFile(TinyTraceBytes, b)
// }

// func BenchmarkGetBigTrace(b *testing.B) {
// 	benchmarkGetTrace(b, BigTrace)
// }

// func BenchmarkGetSmallTrace(b *testing.B) {
// 	benchmarkGetTrace(b, SmallTrace)
// }

// func BenchmarkGetTinyTrace(b *testing.B) {
// 	benchmarkGetTrace(b, TinyTrace)
// }

// func BenchmarkPutTwoBytes(b *testing.B) {
// 	benchmarkWriteFile(TwoBytes, b)
// }

// func BenchmarkPutHundredBytes(b *testing.B) {
// 	benchmarkWriteFile(HundredBytes, b)
// }

// func BenchmarkPutThousandBytes(b *testing.B) {
// 	benchmarkWriteFile(ThousandBytes, b)
// }

// func BenchmarkPutTenThousandBytes(b *testing.B) {
// 	benchmarkWriteFile(TenThousandBytes, b)
// }

// func BenchmarkPutHundredThousandBytes(b *testing.B) {
// 	benchmarkWriteFile(HundredThousandBytes, b)
// }

// func BenchmarkPutMegaBytes(b *testing.B) {
// 	benchmarkWriteFile(MegaBytes, b)
// }

// func BenchmarkPutBigTraceBytes(b *testing.B) {
// 	benchmarkWriteFile(BigTraceBytes, b)
// }

// func BenchmarkPutSmallTraceBytes(b *testing.B) {
// 	benchmarkWriteFile(SmallTraceBytes, b)
// }

// func BenchmarkPutTinyTraceBytes(b *testing.B) {
// 	benchmarkWriteFile(TinyTraceBytes, b)
// }

func BenchmarkGraphQuery(b *testing.B) {
	benchmarkGraphQuery("checkoutservice", "cartservice", "2000ms", 6400, b)
}

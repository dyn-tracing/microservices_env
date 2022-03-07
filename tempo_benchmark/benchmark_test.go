package main
import (
	"testing"
)

const BigTrace = "6009004d2e8ca99b64a9a4e1924e4de3" //31 spans
const SmallTrace = "0a9fc375450a9bbb5fc545ecbf0fda4a" //4 spans
const TinyTrace = "9767ed368bf2053d8ac8c360e799d3f2" //1 span

func benchmarkGetTrace(b *testing.B, traceId string) {
	for i := 0; i < b.N; i++ {
		err, _ := getTrace(traceId)
		if err != nil {
			b.Errorf(err.Error())
		}
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
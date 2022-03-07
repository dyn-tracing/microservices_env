package main
import (
	"testing"
)

func BenchmarkGetTrace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err, _ := getTrace()
		if err != nil {
			b.Errorf(err.Error())
		}
	}
}
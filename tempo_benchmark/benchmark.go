package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

const TempoQueryAddr = "http://34.69.177.136:16686"
const GetTraceEndpoint = "/api/traces"
const TraceId = "/215a45f0eb8a764e99758dac2b9ad8ed" // 31 spans

func getTrace() (error, []byte) {
	resp, err := http.Get(TempoQueryAddr + GetTraceEndpoint + TraceId)
	if err != nil {
		log.Fatal(err)
		return err, nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
		return err, nil
	}

	return nil, body
}
func main() {
	err, trace := getTrace()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(trace))
}
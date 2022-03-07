package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

const TempoQueryAddr = "http://35.193.123.140:16686/"
const GetTraceEndpoint = "api/traces/"

func getTrace(traceId string) (error, []byte) {
	resp, err := http.Get(TempoQueryAddr + GetTraceEndpoint + traceId)
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
	err, trace := getTrace("6009004d2e8ca99b64a9a4e1924e4de3")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(trace))
}
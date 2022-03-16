package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
)

const TempoQueryAddr = "http://104.155.190.158:16686/"
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
	// log.Println(len(body), resp.StatusCode)

	if resp.StatusCode != 200 {
		log.Fatalf("%s Response failed with status code: %d and\nbody: %s\n", traceId, resp.StatusCode, body)
	}
	return nil, body
}

func downloadFileIntoMemory(w io.Writer, bucket, object string, client *storage.Client) ([]byte, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return data, nil
}

func main() {
	err, trace := getTrace("7cd39c7f87b4c84fbea56d3a7c049d05")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(trace))
}

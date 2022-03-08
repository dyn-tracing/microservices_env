package main

import (
    "cloud.google.com/go/storage"
    "context"
	"fmt"
	"io"
    "io/ioutil"
    "log"
    "time"
	"net/http"
)

const TempoQueryAddr = "http://34.72.20.79:16686/"
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

func downloadFileIntoMemory(w io.Writer, bucket, object string, client* storage.Client) ([]byte, error) {
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
	err, trace := getTrace("6009004d2e8ca99b64a9a4e1924e4de3")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(trace))
}

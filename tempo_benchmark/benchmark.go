package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
)

type TraceResult struct {
	TraceID           string
	RootServiceName   string
	DurationMs        int
	StartTimeUnixNano string
}

type MetricsResult struct {
	InspectedTraces int
	InspectedBytes  string
	InspectedBlocks int
}

type SearchResult struct {
	Traces  []TraceResult
	Metrics MetricsResult
}

const TempoQueryAddr = "http://35.239.124.40:16686/"
const TempoAPIAddr = "http://35.239.124.40:3200/"
const GetTraceEndpoint = "api/traces/"
const TempoSearchEndpoint = "api/search"

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

func getTracesHavingTag(tag string, searchInterval string) ([]string, error) {
	endTime := time.Now().Local().Unix()
	startTime := endTime - 3600

	addr := TempoAPIAddr + TempoSearchEndpoint + "?tags=" + tag + "%20start%3D" + strconv.FormatInt(startTime, 10) + "%20end%3D" + strconv.FormatInt(endTime, 10)

	log.Println("addr: ", addr)
	resp, err := http.Get(addr)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	var result SearchResult
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	var traceIds []string
	for _, trace := range result.Traces {
		traceIds = append(traceIds, trace.TraceID)
	}

	return traceIds, nil
}

func takeIntersection(a []string, b []string) ([]string, error) {
	c := []string{}
	m := make(map[string]bool)

	for _, item := range a {
		m[item] = true
	}

	for _, item := range b {
		if _, ok := m[item]; ok {
			c = append(c, item)
		}
	}

	return c, nil
}

func filterOnTheChildRelationshipAndTheCondition(traceIds []string, parentSerive string, childService string, condition string) ([]string, error) {
	log.Println(traceIds)
	return nil, nil
}

func getTracesWhereXCallsY(x string, y string, condition string, searchInterval string) ([]string, error) {
	A, err := getTracesHavingTag("service.name%3D"+x+"%20limit%3D1000", searchInterval)
	if err != nil {
		return nil, err
	}

	B, err := getTracesHavingTag("service.name%3D"+y+"%20limit%3D1000", searchInterval)
	if err != nil {
		return nil, err
	}

	C, err := takeIntersection(A, B)
	if err != nil {
		return nil, err
	}

	D, err := filterOnTheChildRelationshipAndTheCondition(C, x, y, condition)
	if err != nil {
		return nil, err
	}

	return D, nil
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

func shuffleData(data string) string {
	rand.Seed(time.Now().Unix())
	dataRune := []rune(data)
	rand.Shuffle(len(dataRune), func(i, j int) {
		dataRune[i], dataRune[j] = dataRune[j], dataRune[i]
	})
	return string(dataRune)
}

func randStr(n int) string {
	letters := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func uploadDataToGCS(w io.Writer, data string) (int, error) {
	n, err := fmt.Fprint(w, data)
	return n, err
}

func main() {

	// err, trace := getTrace("7cd39c7f87b4c84fbea56d3a7c049d05")
	// if err != nil {
	// 	log.Fatalln(err)
	// }
	// log.Println(string(trace))

	_, _ = getTracesWhereXCallsY("checkoutservice", "cartservice", "", "1h")

}

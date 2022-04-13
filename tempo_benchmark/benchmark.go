package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
)

const TempoQueryAddr = "http://34.70.37.67:16686/"
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

func getTracesHavingTag(tag string, searchInterval string) ([]string, error) {
	return nil, nil
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
	return nil, nil
}

func getTracesWhereXCallsY(x string, y string, condition string, searchInterval string) ([]string, error) {
	A, err := getTracesHavingTag(x, searchInterval)
	if err != nil {
		return nil, err
	}

	B, err := getTracesHavingTag(y, searchInterval)
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
	err, trace := getTrace("7cd39c7f87b4c84fbea56d3a7c049d05")
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(string(trace))

	_, _ = getTracesWhereXCallsY("", "", "", "")
}

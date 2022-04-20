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

const TempoIP = "http://104.197.91.3:"
const TempoQueryAddr = TempoIP + "16686/"
const TempoAPIAddr = TempoIP + "3200/"
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

func getTracesHavingTag(tag string, searchIntervalInSeconds int64) ([]string, error) {
	endTime := time.Now().Local().Unix()
	startTime := endTime - searchIntervalInSeconds

	addr := TempoAPIAddr + TempoSearchEndpoint + "?tags=" + tag + "&start=" + strconv.FormatInt(startTime, 10) + "&end=" + strconv.FormatInt(endTime, 10)
	log.Println(addr)

	resp, err := http.Get(addr)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer resp.Body.Close()

	// log.Println(resp.StatusCode)
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

func doesTraceSatisfiesTheRelationship(traceId string, parentSerive string, childService string) (bool, error) {
	err, traceBytes := getTrace(traceId)
	if err != nil {
		log.Fatal(err)
		return false, err
	}

	discoveredParentIDs := []string{}
	providedParentIDs := []string{}

	var trace map[string]interface{}
	json.Unmarshal(traceBytes, &trace)
	traceData := trace["data"].([]interface{})
	traceJSON := traceData[0].(map[string]interface{})
	spanList := traceJSON["spans"].([]interface{})

	for _, span := range spanList {
		spanMap := span.(map[string]interface{})
		spanID := spanMap["spanID"].(string)
		tags := spanMap["tags"].([]interface{})
		for _, tag := range tags {
			tagMap := tag.(map[string]interface{})
			if tagMap["key"].(string) == "serviceName" {
				switch serviceName := tagMap["value"].(string); serviceName {
				case parentSerive:
					providedParentIDs = append(providedParentIDs, spanID)
				case childService:
					references := spanMap["references"].([]interface{})
					for _, reference := range references {
						referenceMap := reference.(map[string]interface{})
						if referenceMap["refType"].(string) == "CHILD_OF" {
							discoveredParentIDs = append(discoveredParentIDs, referenceMap["spanID"].(string))
						}
					}
				default:
					continue
				}
			}
		}
	}

	commonParentIDs, err := takeIntersection(providedParentIDs, discoveredParentIDs)
	if err != nil {
		log.Fatal(err)
		return false, nil
	}

	isRelationshipRespected := len(commonParentIDs) > 0
	return isRelationshipRespected, nil
}

func filterOnTheChildRelationship(traceIds []string, parentSerive string, childService string) ([]string, error) {
	result := []string{}

	for _, traceId := range traceIds {
		ok, err := doesTraceSatisfiesTheRelationship(traceId, parentSerive, childService)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		if ok {
			result = append(result, traceId)
		}
	}

	return result, nil
}

func getTracesWhereXCallsY(x string, y string, overallLatency string, searchInterval int64) ([]string, error) {
	A, err := getTracesHavingTag(getFormatedTag("service.name", x)+"&limit=1000&maxDuration="+overallLatency, searchInterval)
	// log.Println("getTracesHavingTag: ", len(A))
	if err != nil {
		return nil, err
	}

	B, err := getTracesHavingTag(getFormatedTag("service.name", y)+"&limit=1000&maxDuration="+overallLatency, searchInterval)
	// log.Println("getTracesHavingTag: ", len(B))
	if err != nil {
		return nil, err
	}

	C, err := takeIntersection(A, B)
	// log.Println("takeIntersection: ", len(C))
	if err != nil {
		return nil, err
	}

	D, err := filterOnTheChildRelationship(C, x, y)
	// log.Println("filter: ", len(D))
	if err != nil {
		return nil, err
	}

	return D, nil
}

func getFormatedTag(key string, value string) string {
	return key + "%3D" + value
}

func getConcatenatedTags(tags []string) string {
	var res string
	for _, tag := range tags {
		if res == "" {
			res = tag
		} else {
			res = res + "%20" + tag
		}
	}
	return res
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

	traceIDs, err := getTracesWhereXCallsY("checkoutservice", "cartservice", "2000ms", 6400)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println(traceIDs)
	}
}

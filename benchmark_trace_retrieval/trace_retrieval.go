package main

import (
    "fmt"
    "context"
    "log"
    "time"
    "bytes"
    "io"
    "io/ioutil"
    "os"

    "cloud.google.com/go/storage"
)

// downloadFileIntoMemory downloads an object.
func downloadFileIntoMemory(w io.Writer, bucket, object string, client* storage.Client) ([]byte, error) {
        // bucket := "bucket-name"
        // object := "object-name"
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
        return bytes.TrimSpace(data), nil
}


// downloadFile downloads an object to a file.
func downloadFile(w io.Writer, bucket, object string, destFileName string, client storage.Client) error {
        // bucket := "bucket-name"
        // object := "object-name"
        // destFileName := "file.txt"
        ctx := context.Background()
        ctx, cancel := context.WithTimeout(ctx, time.Second*50)
        defer cancel()

        f, err := os.Create(destFileName)
        if err != nil {
                return fmt.Errorf("os.Create: %v", err)
        }

        rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
        if err != nil {
                return fmt.Errorf("Object(%q).NewReader: %v", object, err)
        }
        defer rc.Close()

        if _, err := io.Copy(f, rc); err != nil {
                return fmt.Errorf("io.Copy: %v", err)
        }

        if err = f.Close(); err != nil {
                return fmt.Errorf("f.Close: %v", err)
        }
        return nil
}

func getTrace(traceID string, client* storage.Client) (string, error) {
    // Sets the name for the new bucket.
    bucketName := "dyntraces-snicket"
    var buf bytes.Buffer
    var entireTrace string
    traceBuf, err := downloadFileIntoMemory(&buf, bucketName, traceID, client)
    if err != nil {
        return "", fmt.Errorf("downloadFileIntoMemory: %v", err)
    }
    //fmt.Printf("tracebuf is %s\n", bytes.NewBuffer(traceBuf))
    for _, span := range bytes.Split(traceBuf, []byte("\n")) {
        split := bytes.Split(span, []byte(":"))
        if len(split) == 3 {
            newBuf, _ := downloadFileIntoMemory(&buf, string(split[2])+"-snicket", string(split[1]), client)
            entireTrace = entireTrace + bytes.NewBuffer(newBuf).String()
            fmt.Printf("entire trace is %s\n\n\n\n", entireTrace)
        }
    }
    return entireTrace, nil
}

func main() {
    ctx := context.Background()

    // Creates a client.
    client, err := storage.NewClient(ctx)
    if err != nil {
            log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()

    s, _ := getTrace("82b29a11332a18878a7d5664b583d983", client)
    fmt.Printf("entire trace %s\n", s)
}

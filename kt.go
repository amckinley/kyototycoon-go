package main

import (
    "fmt"
    "net/http"
    "strings"
    "bytes"
    "encoding/base64"
    //"os"
    "errors"
)

type RemoteDB struct {
    host string
    port int
    client *http.Client
}

// TSV with Base64 encoding is recommended for arbitrary string and binary
// data because its space efficiency is the best of the three formats.

// dont forget to enable keepalive

// need to support a cursor

// dont forget /rpc/match_prefix

// metaRaw, err := info.DBConn.GetBytes(ktKey); err == nil
//  err := info.DBConn.GetBulkBytes(posKeys); err == nil
// keys, err := info.DBConn.MatchPrefix(ktKey, MAX_RECS); err == nil

// error handling: raise or suppress?


func main() {
    //input := []byte("foo\x00bar")
    // input := "key"
    //input := bytes.NewBufferString("lolwut")
    // byteArray := []byte(input)
    // encoder := base64.NewEncoder(base64.StdEncoding, os.Stdout)
    // encoder.Write(byteArray)
    // encoder.Close()

    //res := Set("katy", "perry")
    //res2 := Get("katy")

    d, err := Open("localhost", 1978, -1.0)
    res := d.Set("teenage", "foobar")
    res2 := d.Get("teenage")

    fmt.Printf("Hello, world.\n", res, res2, d, err, "\n")
}


func Open(host string, port int, timeout float32) (*RemoteDB, error) {
        d := &RemoteDB{host, port, nil}
        d.client = &http.Client{}
        return d, nil
}


func (d *RemoteDB) GetBytes(key string) ([]byte, error) {
    b := []byte("bar")
    return b, errors.New("foo")
}

// XXX: this is probably doing tons of spurious string copying
func convertMapToTSV(cols map[string]string) (string) {
    var buffer bytes.Buffer
    for key, value := range cols {
        key_b64 := base64.StdEncoding.EncodeToString([]byte(key))
        value_b64 := base64.StdEncoding.EncodeToString([]byte(value))
        buffer.WriteString(fmt.Sprintf("%s\t%s\n", key_b64, value_b64))
    }

    return buffer.String()
}

// XXX: return type confusion
func (d *RemoteDB) makeRequest(method string, body string) (*http.Request) {
    url := fmt.Sprintf("http://%s:%d/rpc/%s", d.host, d.port, method)
    req, _ := http.NewRequest("POST", url, strings.NewReader(body))

    req.Header.Set("Content-Type", "text/tab-separated-values; colenc=B")
    return req
}

func (d *RemoteDB) Set(key string, value string) (string) {
    payload := make(map[string]string)
    payload["key"] = key
    payload["value"] = value

    body := convertMapToTSV(payload)
    req := d.makeRequest("set", body)

    resp, _ := d.client.Do(req)
    defer resp.Body.Close()

    buf := new(bytes.Buffer)
    buf.ReadFrom(resp.Body)
    s := buf.String()
    return s
}

func (d *RemoteDB) Get(key string) (string) {
    payload := make(map[string]string)
    payload["key"] = key

    body := convertMapToTSV(payload)
    req := d.makeRequest("get", body)

    resp, _ := d.client.Do(req)
    defer resp.Body.Close()

    buf := new(bytes.Buffer)
    buf.ReadFrom(resp.Body)
    s := buf.String()

    chunks := strings.Split(s, "\t")

    for _, c := range chunks {
        fmt.Printf("chunk '%s' \n", strings.TrimSpace(c))
    }
    return s

}


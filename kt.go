package main

import (
    "fmt"
    "net/http"
    "strings"
    "bytes"
    "encoding/base64"
    "strconv"
)

type RemoteDB struct {
    host string
    port int
    client *http.Client
}

func main() {
    d, _ := Open("localhost", 1978, -1.0)

    // test Set()
    d.Set("katy", "perry")
    d.Set("teenage", "dream")
    d.Set("teenager", "val2")
    d.Set("teenage_dream", "val3")

    // test Get()
    res1 := d.Get("teenage")
    if res1 != "dream" {
        panic("bad Get")
    }

    // test MatchPrefix()
    res2, _ := d.MatchPrefix("teenage", -1)
    if len(res2) != 3 {
        panic("bad MatchPrefix")
    }

    // test GetBulkBytes()
    bb_map := make(map[string][]byte)
    bb_map["teenage"] = nil
    bb_map["teenager"] = nil
    bb_map["miley"] = nil

    d.GetBulkBytes(bb_map)
    if !bytes.Equal(bb_map["teenage"], []byte("dream")) {
        panic("bad GetBulkBytes")
    }

    // test GetBytes
    res3, _ := d.GetBytes("teenager")
    var expected = []byte{100, 114, 101, 97, 109}

    if bytes.Equal(expected, res3) {
        panic("bad GetBytes")
    }
}

func Open(host string, port int, timeout float32) (*RemoteDB, error) {
    d := &RemoteDB{host, port, nil}
    d.client = &http.Client{}
    return d, nil
}

func (d *RemoteDB) MatchPrefix(prefix string, max int64) ([]string, error) {
    payload := make(map[string]string)
    payload["prefix"] = prefix
    payload["max"] = strconv.FormatInt(max, 10)

    body := convertMapToTSV(payload)
    req := d.makeRequest("match_prefix", body)

    resp, err := d.client.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()

    buf := new(bytes.Buffer)
    buf.ReadFrom(resp.Body)
    s := buf.String()
    lines := strings.Split(s, "\n")

    matches := make([]string, 0, len(lines)-1)
    for _, line := range lines {
        if line == "" { break }

        // format is "key_name \t useless_idx"
        chunks := strings.Split(line, "\t")

        // currently KT puts the number of retrieved keys at the *end*
        // of the results, which is useless, but maybe they'll move it
        if chunks[0] == "num" { continue }

        matches = append(matches, chunks[0])
    }

    return matches, nil
}

// GetBulk returns all values for the passed in array of keys. If a key does
// not exist, the value for this key is set to empty string. If the key does
// exist, the value in the passed in map is set accordingly.
func (d *RemoteDB) GetBulkBytes(keysAndVals map[string][]byte) (error) {
    key_list := make([]string, 0, len(keysAndVals))

    for k, _ := range keysAndVals {
        key_list = append(key_list, k)
    }

    body := convertArrayToTSV(key_list)
    req := d.makeRequest("get_bulk", body)

    resp, err := d.client.Do(req)
    if err != nil { return err }
    defer resp.Body.Close()

    buf := new(bytes.Buffer)
    buf.ReadFrom(resp.Body)
    s := buf.String()
    lines := strings.Split(s, "\n")

    for _, line := range lines {
        if line == "" { break }

        // format is "_keyName \t keyVal" (notice the leading underscore)
        chunks := strings.Split(line, "\t")
        key := chunks[0][1:]
        value := chunks[1]

        // currently KT puts the number of retrieved keys at the *end*
        // of the results, which is useless, but maybe they'll move it
        if chunks[0] == "num" { continue }

        if _, ok := keysAndVals[key]; ok {
            keysAndVals[key] = []byte(value)
        }
    }

    // add empty values. XXX: necessary?
    for key, value := range keysAndVals {
        if value == nil {
            keysAndVals[key] = []byte("")
        }
    }
    return nil
}

func (d *RemoteDB) Set(key string, value string) (string) {
    payload := make(map[string]string)
    payload["key"] = key
    payload["value"] = value

    body := convertMapToTSV(payload)
    req := d.makeRequest("set", body)

    resp, err := d.client.Do(req)
    if err != nil { panic(err) }
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

    resp, err := d.client.Do(req)
    if err != nil { panic(err) }
    defer resp.Body.Close()

    buf := new(bytes.Buffer)
    buf.ReadFrom(resp.Body)
    s := buf.String()

    chunks := strings.Split(s, "\t")
    return strings.TrimSpace(chunks[1])
}

func (d *RemoteDB) GetBytes(key string) ([]byte, error) {
    return []byte(d.Get(key)), nil
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

// XXX: this is adding leading underscores to every key. maybe not a good idea?
func convertArrayToTSV(arr []string) (string) {
    var buffer bytes.Buffer
    for _, val := range arr {
        prefixed_val := fmt.Sprintf("_%s", val)
        val_b64 := base64.StdEncoding.EncodeToString([]byte(prefixed_val))
        buffer.WriteString(fmt.Sprintf("%s\t\n", val_b64))
    }
    return buffer.String()
}

// XXX: return type confusion?
func (d *RemoteDB) makeRequest(method string, body string) (*http.Request) {
    url := fmt.Sprintf("http://%s:%d/rpc/%s", d.host, d.port, method)
    req, _ := http.NewRequest("POST", url, strings.NewReader(body))

    req.Header.Set("Content-Type", "text/tab-separated-values; colenc=B")

    return req
}


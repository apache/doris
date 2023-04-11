/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package doris

import (
    "context"
    "encoding/base64"
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "net/http"
    "strings"
    "testing"
    "time"

    "gotest.tools/assert"

    "github.com/elastic/beats/v7/libbeat/beat"
    "github.com/elastic/beats/v7/libbeat/common"
    "github.com/elastic/beats/v7/libbeat/outputs"
    _ "github.com/elastic/beats/v7/libbeat/outputs/codec/format"
    _ "github.com/elastic/beats/v7/libbeat/outputs/codec/json"
    "github.com/elastic/beats/v7/libbeat/outputs/outest"
)

var fenodes = flag.String("fenodes", "http://localhost:8030", "fe node address")
var user = flag.String("user", "root", "doris user")
var password = flag.String("password", "", "doris password")
var database = flag.String("database", "test", "doris database")

func TestDorisPublish(t *testing.T) {
    dorisConfig := map[string]interface{}{
        "fenodes":  strings.Split(*fenodes, ","),
        "user":     *user,
        "password": *password,
        "database": *database,
    }
    fmt.Println(dorisConfig)
    testDorisPublishMessage(t, dorisConfig)
}

type eventInfo struct {
    events []beat.Event
}

func makeConfig(t *testing.T, in map[string]interface{}) *common.Config {
    cfg, err := common.NewConfigFrom(in)
    if err != nil {
        t.Fatal(err)
    }
    return cfg
}

func flatten(infos []eventInfo) []beat.Event {
    var out []beat.Event
    for _, info := range infos {
        out = append(out, info.events...)
    }
    return out
}

func testDorisPublishMessage(t *testing.T, cfg map[string]interface{}) {
    tests := []struct {
        title  string
        config map[string]interface{}
        events []eventInfo
    }{
        {
            "test csv events",
            map[string]interface{}{
                "table":               "test_beats_csv",
                "codec_format_string": "%{[message]},%{[@timestamp]},%{[@metadata.type]}",
                "headers": map[string]interface{}{
                    "column_separator": ",",
                },
            },
            []eventInfo{
                {
                    events: []beat.Event{
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "message": "1,A",
                            },
                        },
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "message": "2,B",
                            },
                        },
                    },
                },
            },
        },
        {
            "test json events",
            map[string]interface{}{
                "table":               "test_beats_json",
                "codec_format_string": "%{[message]}",
                "headers": map[string]interface{}{
                    "format":            "json",
                    "read_json_by_line": true,
                },
            },
            []eventInfo{
                {
                    events: []beat.Event{
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "message": "{\"id\": 1, \"name\": \"A\"}",
                            },
                        },
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "message": "{\"id\": 2, \"name\": \"B\"}",
                            },
                        },
                    },
                },
            },
        },
        {
            "test codec events",
            map[string]interface{}{
                "table":               "test_beats_codec",
                "codec_format_string": "{\"id\": %{[fields.id]}, \"name\": \"%{[fields.name]}\", \"timestamp\": \"%{[@timestamp]}\", \"type\": \"%{[@metadata.type]}\"}",
                "headers": map[string]interface{}{
                    "format":            "json",
                    "read_json_by_line": true,
                },
            },
            []eventInfo{
                {
                    events: []beat.Event{
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "fields": common.MapStr{
                                    "id":   1,
                                    "name": "A",
                                },
                            },
                        },
                        {
                            Timestamp: time.Now(),
                            Meta: common.MapStr{
                                "beat":    "filebeat",
                                "type":    "_doc",
                                "version": "7.4.2",
                            },
                            Fields: common.MapStr{
                                "fields": common.MapStr{
                                    "id":   2,
                                    "name": "B",
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    for i, test := range tests {
        config := makeConfig(t, cfg)
        if test.config != nil {
            config.Merge(makeConfig(t, test.config))
        }

        name := fmt.Sprintf("run test(%v): %v", i, test.title)
        t.Run(name, func(t *testing.T) {
            grp, err := makeDoris(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), config)
            if err != nil {
                t.Fatal(err)
            }

            output := grp.Clients[0].(outputs.NetworkClient)
            if err := output.Connect(); err != nil {
                t.Fatal(err)
            }
            defer output.Close()
            // publish test events
            for i := range test.events {
                batch := outest.NewBatch(test.events[i].events...)

                output.Publish(context.Background(), batch)
            }

            expected := flatten(test.events)
            stored := testReadFromDoris(t, config)
            assert.Equal(t, len(expected), stored)
        })
    }
}

type httpResponse struct {
    data map[string]int64 `json:"data"`
}

func testReadFromDoris(t *testing.T, config *common.Config) int64 {
    fenode, feErr := config.String("fenodes", 0)
    if feErr != nil {
        t.Fatal(feErr)
    }
    user, userErr := config.String("user", -1)
    if userErr != nil {
        t.Fatal(userErr)
    }
    password, pwErr := config.String("password", -1)
    if pwErr != nil {
        t.Fatal(pwErr)
    }
    database, dbErr := config.String("database", -1)
    if dbErr != nil {
        t.Fatal(dbErr)
    }
    table, tableErr := config.String("table", -1)
    if tableErr != nil {
        t.Fatal(tableErr)
    }

    getTableCountURL := fmt.Sprintf("%s/api/rowcount?db=%s&table=%s", fenode, database, table)
    request, requestErr := http.NewRequest(http.MethodGet, getTableCountURL, strings.NewReader(""))
    request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+password)))
    if requestErr != nil {
        t.Fatal(requestErr)
    }
    resp, responseErr := http.DefaultClient.Do(request)
    if responseErr != nil || resp.StatusCode >= 300 || resp.StatusCode < 200 {
        t.Fatal(responseErr)
    }

    defer resp.Body.Close()
    body, readErr := ioutil.ReadAll(resp.Body)
    if readErr != nil {
        t.Fatal(readErr)
    }
    var response httpResponse
    if jsonErr := json.Unmarshal(body, &response); jsonErr != nil {
        t.Fatal(jsonErr)
    }
    return response.data[table]
}

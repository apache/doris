// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/spf13/cast"
)

var (
	dorisStreamLoadURL string
	dorisUsername      string
	dorisPassword      string
)

func init() {
	dorisStreamLoadURL = "http://{fe_host}:{fe_port}/api/{db_name}/{table_name}/_stream_load"
	dorisUsername = "{doris_username}"
	dorisPassword = "{doris_password}"
}

func SubmitStreamLoadData(ctx context.Context, crowd, logID, num int64, body, partition string) error {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				req.SetBasicAuth(dorisUsername, dorisPassword)
				return nil, nil
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", dorisStreamLoadURL, strings.NewReader(body))
	if err != nil {
		log.Fatalf("[stream load failed err=%+v]\n", err)
		return err
	}
	req.Header.Add("columns", fmt.Sprintf("member_id,confidence=100,bucket=floor(member_id/1000000),tag_range=%d,tag_group=%d,tag_value_id='-1',members=to_bitmap(member_id),partition_sign='%s'", logID, crowd, partition))
	req.Header.Add("format", "json")
	req.Header.Add("Label", fmt.Sprintf("crowd_%d_%d_%d", crowd, logID, num))
	req.Header.Add("jsonpaths", "[\"$.member_id\"]")
	req.Header.Add("Expect", "100-continue")
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("strip_outer_array", "true")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// Read Response Body
	respBody, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("stream load：code=%d, body=%s", resp.StatusCode, string(respBody))
	}

	res := make(map[string]interface{})
	err = json.Unmarshal(respBody, &res)
	if err != nil {
		return err
	}
	// Display Results
	log.Printf("[response Status : %+v]", resp.Status)
	log.Printf("[response Headers : %+v]", resp.Header)
	log.Printf("[response Body : %s]", string(respBody))
	if cast.ToString(res["Status"]) != "Success" {
		return fmt.Errorf("mas=%s, Label = %s", cast.ToString(res["Message"]), cast.ToString(res["Label"]))
	}
	return nil
}

func main() {
	err := SubmitStreamLoadData(context.Background(), 1, 1, 1, `{"member_id": 1}`, "2020-01-01")
	if err != nil {
		log.Fatalf("[stream load failed err=%+v]\n", err)
	}
}

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
	"fmt"
	"net"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/outputs"
	_ "github.com/elastic/beats/v7/libbeat/outputs/codec/format" // codec format
	_ "github.com/elastic/beats/v7/libbeat/outputs/codec/json"   // codec json
	"github.com/elastic/beats/v7/libbeat/outputs/outest"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/stretchr/testify/require"
)

type Attr struct {
	Table   string
	Message string
	Result  string
	Const   bool
}

func TestSelector(t *testing.T) {
	now := time.Now()
	fmt.Println(now)

	attrs := []*Attr{
		{"tablE", "123", "tablE", true},
		{"table_%{[message]}", "123", "table_123", false},
		{"table_%{+yyyy-MM-dd}", "123", fmt.Sprintf("table_%s", now.Format("2006-01-02")), false},
		{"table_%{[message]}", "%{[message]}", "table_%{[message]}", false},
	}

	for i, attr := range attrs {
		fmt.Printf("table%v: %v\n", i, attr.Table)

		dorisConfig := map[string]interface{}{
			"fenodes":  []string{"http://localhost:8030"},
			"user":     "admin",
			"password": "",
			"database": "db",
			"table":    attr.Table,
		}

		cfg, err := common.NewConfigFrom(dorisConfig)
		require.NoError(t, err)
		_, err = makeDoris(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
		require.NoError(t, err)

		tableSelector, err := buildTableSelector(cfg)
		require.NoError(t, err)
		require.Equal(t, tableSelector.Sel.IsConst(), attr.Const)

		event := &beat.Event{
			Timestamp: now,
			Fields: common.MapStr{
				"message": attr.Message,
			},
		}
		s, err := tableSelector.Sel.Select(event)
		fmt.Printf("result: %v\n", s)
		require.NoError(t, err)
		require.Equal(t, attr.Result, s)
	}
}

func TestSelectorWithDefaultTable(t *testing.T) {
	now := time.Now()
	fmt.Println(now)

	attrs := []*Attr{
		{"", "123", "DEFAULT", false},
		{"tablE", "123", "tablE", true},
		{"table_%{[message]}", "123", "table_123", false},
		{"table_%{+yyyy-MM-dd}", "123", fmt.Sprintf("table_%s", now.Format("2006-01-02")), false},
		{"table_%{[message]}", "%{[message]}", "table_%{[message]}", false},
		{"table_%{[no_such_field]}", "any_thing", "DEFAULT", false},
	}

	for i, attr := range attrs {
		fmt.Printf("table%v: %v\n", i, attr.Table)

		dorisConfig := map[string]interface{}{
			"fenodes":  []string{"http://localhost:8030"},
			"user":     "admin",
			"password": "",
			"database": "db",
			"tables": []map[string]interface{}{
				{
					"table":   attr.Table,
					"default": "DEFAULT",
				},
			},
		}

		cfg, err := common.NewConfigFrom(dorisConfig)
		require.NoError(t, err)
		_, err = makeDoris(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
		require.NoError(t, err)

		tableSelector, err := buildTableSelector(cfg)
		require.NoError(t, err)
		require.Equal(t, tableSelector.Sel.IsConst(), attr.Const)

		event := &beat.Event{
			Timestamp: now,
			Fields: common.MapStr{
				"message": attr.Message,
			},
		}
		s, err := tableSelector.Sel.Select(event)
		fmt.Printf("result: %v\n", s)
		require.NoError(t, err)
		require.Equal(t, attr.Result, s)
	}
}

func TestWorkers(t *testing.T) {
	dorisConfig := map[string]interface{}{
		"fenodes":  []string{"http://localhost:8030", "http://localhost:8031"},
		"user":     "admin",
		"password": "",
		"database": "db",
		"table":    "table",
		"worker":   3,
	}

	cfg, err := common.NewConfigFrom(dorisConfig)
	require.NoError(t, err)
	group, err := makeDoris(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
	require.NoError(t, err)
	require.Equal(t, 2*3, len(group.Clients))
}

func TestMultiTable(t *testing.T) {
	database := "log_db"
	table := "%{[message]}"

	messages := []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}
	var events []beat.Event
	for _, m := range messages {
		events = append(events, beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"message": fmt.Sprintf("t_%d", m),
			},
		})
	}
	// retry with default table
	events = append(events, beat.Event{Fields: common.MapStr{"message": ""}})
	events = append(events, beat.Event{Fields: common.MapStr{"message": ""}})
	// will be dropped, because of serialization error
	events = append(events, beat.Event{Fields: common.MapStr{}})
	events = append(events, beat.Event{Fields: common.MapStr{}})

	apiList := []*api{
		{database: database, table: "t_1", status: "Success"},
		{database: database, table: "t_2", status: "Publish Timeout"},
		{database: database, table: "t_3", status: "Label Already Exists"},
		{database: database, table: "t_4", status: "Fail"},
		{database: database, table: "t_5", status: "Fail"},
		{database: database, table: "DEFAULT", status: "Fail"},
	}

	port, err := findRandomPort()
	require.NoError(t, err)
	server := mockDorisServer(port, apiList)
	fmt.Printf("mock doris server started on localhost:%d\n", port)
	defer server.Close()

	dorisConfig := map[string]interface{}{
		"fenodes":             []string{fmt.Sprintf("http://localhost:%d", port)},
		"user":                "root",
		"password":            "",
		"database":            database,
		"table":               table,
		"default_table":       "DEFAULT",
		"codec_format_string": "%{[message]}",
		"headers": map[string]interface{}{
			"format":            "json",
			"read_json_by_line": true,
		},
	}

	cfg, err := common.NewConfigFrom(dorisConfig)
	require.NoError(t, err)
	grp, err := makeDoris(nil, beat.Info{Beat: "libbeat"}, outputs.NewNilObserver(), cfg)
	require.NoError(t, err)

	output := grp.Clients[0].(outputs.NetworkClient)
	err = output.Connect()
	require.NoError(t, err)
	defer output.Close()

	batch := outest.NewBatch(events[:2]...) // Success
	err0 := fmt.Errorf("Not Started")
	for i := 0; err0 != nil && i < 10; i++ { // until server started
		err0 = output.Publish(context.Background(), batch)
		fmt.Printf("%+v\n", err0)
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err0)
	require.Equal(t, outest.BatchACK, batch.Signals[len(batch.Signals)-1].Tag)

	batch = outest.NewBatch(events[:4]...) // Success + Publish Timeout
	err0 = output.Publish(context.Background(), batch)
	require.NoError(t, err0)
	require.Equal(t, outest.BatchACK, batch.Signals[len(batch.Signals)-1].Tag)

	batch = outest.NewBatch(events[:6]...) // Success + Publish Timeout + Label Already Exists
	err0 = output.Publish(context.Background(), batch)
	require.NoError(t, err0)
	require.Equal(t, outest.BatchACK, batch.Signals[len(batch.Signals)-1].Tag)

	batch = outest.NewBatch(events[:12]...) // Success + Publish Timeout + Label Already Exists + Fail
	err0 = output.Publish(context.Background(), batch)
	require.Error(t, err0)
	require.Equal(t, outest.BatchRetryEvents, batch.Signals[len(batch.Signals)-1].Tag)
	require.Equal(t, 6, len(batch.Signals[len(batch.Signals)-1].Events)) // 6 retry events

	retryEvents := make([]beat.Event, 0)
	for _, e := range batch.Signals[len(batch.Signals)-1].Events {
		retryEvents = append(retryEvents, e.Content)
	}

	tables := make([]string, 0)
	barrier, err0 := getBarrierFromEvent(&publisher.Event{Content: retryEvents[0]}) // has barrier
	require.NoError(t, err0)
	tables = append(tables, barrier.Table)
	barrier, err0 = getBarrierFromEvent(&publisher.Event{Content: retryEvents[2]}) // has barrier
	require.NoError(t, err0)
	tables = append(tables, barrier.Table)
	barrier, err0 = getBarrierFromEvent(&publisher.Event{Content: retryEvents[4]}) // has barrier
	require.NoError(t, err0)
	tables = append(tables, barrier.Table)

	sort.Slice(tables, func(i, j int) bool {
		return tables[i] < tables[j]
	})
	require.Equal(t, fmt.Sprintf("%v", []string{"DEFAULT", "t_4", "t_5"}), fmt.Sprintf("%v", tables))

	batch = outest.NewBatch(retryEvents...) // Fail (retry)
	err0 = output.Publish(context.Background(), batch)
	require.Error(t, err0)
	require.Equal(t, outest.BatchRetryEvents, batch.Signals[len(batch.Signals)-1].Tag)
	require.Equal(t, 6, len(batch.Signals[len(batch.Signals)-1].Events)) // 6 retry events

	batch = outest.NewBatch(events[12:14]...) // Dropped before sending
	err0 = output.Publish(context.Background(), batch)
	require.NoError(t, err0)
	require.Equal(t, outest.BatchACK, batch.Signals[len(batch.Signals)-1].Tag)
}

type api struct {
	database string
	table    string
	status   string
}

func mockDorisServer(port int, apiList []*api) *http.Server {
	server := &http.Server{
		ReadTimeout: 3 * time.Second,
		Addr:        fmt.Sprintf(":%d", port),
	}

	mux := http.NewServeMux()
	for _, api := range apiList {
		url := fmt.Sprintf("/api/%s/%s/_stream_load", api.database, api.table)
		response := fmt.Sprintf(`{"Status":"%s"}`, api.status)
		mux.HandleFunc(url, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(response))
		})
	}

	server.Handler = mux
	go func() {
		_ = server.ListenAndServe()
	}()

	return server
}

func findRandomPort() (int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	port := l.Addr().(*net.TCPAddr).Port

	err = l.Close()
	if err != nil {
		return 0, err
	}

	return port, nil
}

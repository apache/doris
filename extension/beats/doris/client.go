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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync/atomic"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/google/uuid"
)

type client struct {
	url        string
	httpClient *http.Client
	headers    map[string]string
	beat       beat.Info
	codec      codec.Codec

	database      string
	table         string
	labelPrefix   string
	lineDelimiter string
	logRequest    bool

	observer outputs.Observer
	reporter *ProgressReporter
	logger   *logp.Logger
}

type clientSettings struct {
	URL     string
	Timeout time.Duration
	Headers map[string]string

	Database      string
	Table         string
	LabelPrefix   string
	LineDelimiter string
	LogRequest    bool

	Beat     beat.Info
	Codec    codec.Codec
	Observer outputs.Observer
	Reporter *ProgressReporter
	Logger   *logp.Logger
}

func (s clientSettings) String() string {
	return fmt.Sprintf("clientSettings{%s, %s, %s, %s}", s.URL, s.Timeout, s.LabelPrefix, s.Headers)
}

type ResponseStatus struct {
	Status string `json:"Status"`
}

func (e *ResponseStatus) Error() string { return e.Status }

type ProgressReporter struct {
	totalBytes int64
	totalRows  int64
	failedRows int64
	interval   time.Duration
	logger     *logp.Logger
}

func NewProgressReporter(interval int, logger *logp.Logger) *ProgressReporter {
	return &ProgressReporter{
		totalBytes: 0,
		totalRows:  0,
		failedRows: 0,
		interval:   time.Duration(interval) * time.Second,
		logger:     logger,
	}
}

func (reporter *ProgressReporter) IncrTotalBytes(bytes int64) {
	atomic.AddInt64(&reporter.totalBytes, bytes)
}

func (reporter *ProgressReporter) IncrTotalRows(rows int64) {
	atomic.AddInt64(&reporter.totalRows, rows)
}

func (reporter *ProgressReporter) IncrFailedRows(rows int64) {
	atomic.AddInt64(&reporter.totalRows, rows)
}

func (reporter *ProgressReporter) Report() {
	init_time := time.Now().Unix()
	last_time := init_time
	last_bytes := atomic.LoadInt64(&reporter.totalBytes)
	last_rows := atomic.LoadInt64(&reporter.totalRows)

	reporter.logger.Infof("start progress reporter with interval %v", reporter.interval)
	for reporter.interval > 0 {
		time.Sleep(reporter.interval)

		cur_time := time.Now().Unix()
		cur_bytes := atomic.LoadInt64(&reporter.totalBytes)
		cur_rows := atomic.LoadInt64(&reporter.totalRows)
		total_time := cur_time - init_time
		total_speed_mbps := cur_bytes / 1024 / 1024 / total_time
		total_speed_rps := cur_rows / total_time

		inc_bytes := cur_bytes - last_bytes
		inc_rows := cur_rows - last_rows
		inc_time := cur_time - last_time
		inc_speed_mbps := inc_bytes / 1024 / 1024 / inc_time
		inc_speed_rps := inc_rows / inc_time

		reporter.logger.Infof("total %v MB %v ROWS, total speed %v MB/s %v R/s, last %v seconds speed %v MB/s %v R/s",
			cur_bytes/1024/1024, cur_rows, total_speed_mbps, total_speed_rps,
			inc_time, inc_speed_mbps, inc_speed_rps)

		last_time = cur_time
		last_bytes = cur_bytes
		last_rows = cur_rows
	}
}

func NewDorisClient(s clientSettings) (*client, error) {
	s.Logger.Infof("Received settings: %s", s)

	client := &client{
		url: s.URL,
		httpClient: &http.Client{
			Timeout: s.Timeout,
		},
		headers: s.Headers,

		database:      s.Database,
		table:         s.Table,
		labelPrefix:   s.LabelPrefix,
		lineDelimiter: s.LineDelimiter,
		logRequest:    s.LogRequest,

		codec:    s.Codec,
		observer: s.Observer,
		reporter: s.Reporter,
		logger:   s.Logger,
	}
	return client, nil
}

func (client *client) Connect() error {
	return nil
}

func (client *client) Close() error {
	return nil
}

func (client *client) String() string {
	return fmt.Sprintf("doris{%s, %s, %s}", client.url, client.labelPrefix, client.headers)
}

func (client *client) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	length := len(events)
	client.logger.Debugf("Received events: %d", length)

	label := fmt.Sprintf("%s_%s_%s_%d_%s", client.labelPrefix, client.database, client.table, time.Now().UnixMilli(), uuid.New())
	rest, err := client.publishEvents(label, events)

	if len(rest) == 0 {
		batch.ACK()
		client.logger.Debugf("Success send %d events", length)
	} else {
		client.observer.Failed(length)
		batch.RetryEvents(rest)
		client.logger.Warnf("Retry send %d events", length)
	}
	return err
}

func (client *client) publishEvents(lable string, events []publisher.Event) ([]publisher.Event, error) {
	begin := time.Now()

	var logFirstEvent []byte
	var stringBuilder strings.Builder

	dropped := 0
	for i := range events {
		event := &events[i]
		serializedEvent, err := client.codec.Encode(client.beat.Beat, &event.Content)

		if err != nil {
			if event.Guaranteed() {
				client.logger.Errorf("Failed to serialize the event: %+v", err)
			} else {
				client.logger.Warnf("Failed to serialize the event: %+v", err)
			}
			client.logger.Debugf("Failed event: %v", event)

			dropped++
			client.reporter.IncrFailedRows(1)
			continue
		}

		if logFirstEvent == nil {
			logFirstEvent = serializedEvent
		}
		stringBuilder.Write(serializedEvent)
		stringBuilder.WriteString(client.lineDelimiter)
	}
	request, requestErr := http.NewRequest(http.MethodPut, client.url, strings.NewReader(stringBuilder.String()))
	if requestErr != nil {
		client.logger.Errorf("Failed to create request: %s", requestErr)
		return events, requestErr
	}

	var groupCommit bool = false
	for k, v := range client.headers {
		request.Header.Set(k, v)
		if k == "group_commit" && v != "off_mode" {
			groupCommit = true
		}
	}
	if !groupCommit {
		request.Header.Set("label", lable)
	}

	response, responseErr := client.httpClient.Do(request)
	if responseErr != nil {
		client.logger.Errorf("Failed to stream-load request: %v", responseErr)
		return events, responseErr
	}

	defer response.Body.Close()

	responseBytes, responseErr := httputil.DumpResponse(response, true)
	if responseErr != nil {
		client.logger.Errorf("Failed to dump doris stream load response: %v, error: %v", response, responseErr)
		return events, responseErr
	}

	if client.logRequest {
		client.logger.Infof("doris stream load response response:\n%s", string(responseBytes))
	}

	body, bodyErr := ioutil.ReadAll(response.Body)
	if bodyErr != nil {
		client.logger.Errorf("Failed to read doris stream load response body, error: %v, response:\n%v", bodyErr, string(responseBytes))
		return events, bodyErr
	}

	var status ResponseStatus
	parseErr := json.Unmarshal(body, &status)
	if parseErr != nil {
		client.logger.Errorf("Failed to parse doris stream load response to JSON, error: %v, response:\n%v", parseErr, string(responseBytes))
		return events, parseErr
	}

	if status.Status != "Success" {
		client.logger.Errorf("doris stream load status: '%v' is not 'Success', full response: %v", status.Status, string(responseBytes))
		return events, &status
	}

	client.logger.Debugf("Stream-Load publish events: %d events have been published to doris in %v.",
		len(events)-dropped,
		time.Now().Sub(begin))

	client.observer.Dropped(dropped)
	client.observer.Acked(len(events) - dropped)

	client.reporter.IncrTotalBytes(int64(stringBuilder.Len()))
	client.reporter.IncrTotalRows(int64(len(events) - dropped))

	return nil, nil
}

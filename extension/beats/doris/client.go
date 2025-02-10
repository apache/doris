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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
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
	dbURL      string
	httpClient *http.Client
	headers    map[string]string
	beat       beat.Info
	codec      codec.Codec

	database      string
	tableSelector *fmtSelector
	labelPrefix   string
	lineDelimiter string
	logRequest    bool

	observer outputs.Observer
	reporter *ProgressReporter
	logger   *logp.Logger
}

var _ outputs.NetworkClient = (*client)(nil)

type clientSettings struct {
	DBURL   string
	Timeout time.Duration
	Headers map[string]string

	Database      string
	TableSelector *fmtSelector
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
	str := fmt.Sprintf("clientSettings{%s/{table}/_stream_load, %s, %s, %s}", s.DBURL, s.Timeout, s.LabelPrefix, s.Headers)
	if _, ok := s.Headers["Authorization"]; ok {
		return strings.Replace(str, "Authorization:"+s.Headers["Authorization"], "Authorization:Basic ******", 1)
	}
	return str
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
		dbURL: s.DBURL,
		httpClient: &http.Client{
			Timeout: s.Timeout,
		},
		headers: s.Headers,

		database:      s.Database,
		tableSelector: s.TableSelector,
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
	str := fmt.Sprintf("doris{%s, %s, %s}", client.url("{table}"), client.labelPrefix, client.headers)
	if _, ok := client.headers["Authorization"]; ok {
		return strings.Replace(str, "Authorization:"+client.headers["Authorization"], "Authorization:Basic ******", 1)
	}
	return str
}

func (client *client) url(table string) string {
	return fmt.Sprintf("%s/%s/_stream_load", client.dbURL, table)
}

func (client *client) label(table string) string {
	return fmt.Sprintf("%s_%s_%s_%d_%s", client.labelPrefix, client.database, table, time.Now().UnixMilli(), uuid.New())
}

// Publish sends events to doris.
// batch.Events() are grouped by table first (tableEvents).
// For each tableEvents, call the http stream load api to send the tableEvents to doris.
// If a tableEvents returns an error, add a barrier to the last event of the tableEvents.
// A barrier contains a table, a stream load label, and the length of the tableEvents.
// Add all failed tableEvents to the retryEvents.
// So if the last event in the batch.Events() has a barrier, it means that this is a retry.
// In this case, we will split the batch.Events() to some tableEvents by the barrier events
// and send each tableEvents to doris again reusing the label in the barrier.
func (client *client) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	length := len(events)
	client.logger.Debugf("Received events: %d", length)

	tableEventsMap := client.makeTableEventsMap(ctx, events)
	rest, err := client.publishEvents(tableEventsMap)

	if len(rest) == 0 {
		batch.ACK()
	} else {
		batch.RetryEvents(rest)
		client.logger.Warnf("Retry send %d events", len(rest))
	}
	return err
}

const nilTable = ""

type Events struct {
	Label  string
	Events []publisher.Event

	// used in publishEvents
	serialization string
	dropped       int64
	request       *http.Request
	response      *http.Response
	err           error
}

func (client *client) makeTableEventsMap(_ context.Context, events []publisher.Event) map[string]*Events {
	tableEventsMap := make(map[string]*Events)
	if len(events) == 0 {
		return tableEventsMap
	}

	barrier, err := getBarrierFromEvent(&events[len(events)-1])
	if err == nil { // retry
		if client.tableSelector.Sel.IsConst() { // table is const
			removeBarrierFromEvent(&events[len(events)-1])
			tableEventsMap[barrier.Table] = &Events{
				Label:  barrier.Label,
				Events: events,
			}
		} else { // split events by barrier
			for end := len(events); end > 0; {
				barrier, _ := getBarrierFromEvent(&events[end-1])
				removeBarrierFromEvent(&events[end-1])
				start := end - barrier.Length

				tableEventsMap[barrier.Table] = &Events{
					Label:  barrier.Label,
					Events: events[start:end], // should not do any append to the array, because here is a slice of the original array
				}

				end = start
			}
		}
	} else { // first time
		if client.tableSelector.Sel.IsConst() { // table is const
			table, _ := client.tableSelector.Sel.Select(&events[0].Content)
			label := client.label(table)
			tableEventsMap[table] = &Events{
				Label:  label,
				Events: events,
			}
		} else { // select table for each event
			for _, e := range events {
				table, err := client.tableSelector.Sel.Select(&e.Content)
				if err != nil {
					client.logger.Errorf("Failed to select table: %+v", err)
					table = nilTable
				}
				_, ok := tableEventsMap[table]
				if !ok {
					tableEventsMap[table] = &Events{
						Label:  client.label(table),
						Events: []publisher.Event{e},
					}
				} else {
					tableEventsMap[table].Events = append(tableEventsMap[table].Events, e)
				}
			}
		}
	}

	return tableEventsMap
}

func (client *client) publishEvents(tableEventsMap map[string]*Events) ([]publisher.Event, error) {
	begin := time.Now()

	for table, tableEvents := range tableEventsMap {
		events := tableEvents.Events

		if table == nilTable {
			client.logger.Errorf("Invalid table for %v events", len(events))
			tableEvents.dropped = int64(len(events))
			tableEvents.err = fmt.Errorf("invalid table for %v events", len(events))
			continue
		}

		var stringBuilder strings.Builder

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

				tableEvents.dropped++
				continue
			}

			stringBuilder.Write(serializedEvent)
			stringBuilder.WriteString(client.lineDelimiter)
		}

		tableEvents.serialization = stringBuilder.String()

		var requestErr error
		tableEvents.request, requestErr = http.NewRequest(http.MethodPut, client.url(table), strings.NewReader(tableEvents.serialization))
		if requestErr != nil {
			client.logger.Errorf("Failed to create request: %v", requestErr)
			continue
		}

		var groupCommit = false
		for k, v := range client.headers {
			tableEvents.request.Header.Set(k, v)
			if k == "group_commit" && v != "off_mode" {
				groupCommit = true
			}
		}
		if !groupCommit {
			tableEvents.request.Header.Set("label", tableEvents.Label)
		}
	}

	wg := sync.WaitGroup{}
	for _, tableEvents := range tableEventsMap {
		request := tableEvents.request
		if request != nil {
			wg.Add(1)
			go func(e *Events) {
				e.response, e.err = client.httpClient.Do(request)
				wg.Done()
			}(tableEvents)
		}
	}
	wg.Wait()

	for table, tableEvents := range tableEventsMap {
		if table == nilTable {
			continue
		}

		response := tableEvents.response

		if tableEvents.err != nil {
			client.logger.Errorf("Failed to stream-load request: %v", tableEvents.err)
			continue
		}

		defer response.Body.Close()

		var responseBytes []byte
		responseBytes, tableEvents.err = httputil.DumpResponse(response, true)
		if tableEvents.err != nil {
			client.logger.Errorf("Failed to dump doris stream load response: %v, error: %v", response, tableEvents.err)
			continue
		}

		if client.logRequest {
			client.logger.Infof("doris stream load response response:\n%s", string(responseBytes))
		}

		var body []byte
		body, tableEvents.err = ioutil.ReadAll(response.Body)
		if tableEvents.err != nil {
			client.logger.Errorf("Failed to read doris stream load response body, error: %v, response:\n%v", tableEvents.err, string(responseBytes))
			continue
		}

		var status ResponseStatus
		tableEvents.err = json.Unmarshal(body, &status)
		if tableEvents.err != nil {
			client.logger.Errorf("Failed to parse doris stream load response to JSON, error: %v, response:\n%v", tableEvents.err, string(responseBytes))
			continue
		}

		if status.Status != "Success" && status.Status != "Publish Timeout" && status.Status != "Label Already Exists" {
			client.logger.Errorf("doris stream load status: '%v' is not 'Success', full response: %v", status.Status, string(responseBytes))
			tableEvents.err = errors.New("doris stream load status: " + status.Status)
			continue
		}

		if status.Status == "Label Already Exists" {
			client.logger.Warnf("doris stream load status: '%v', %v events skipped", status.Status, int64(len(tableEvents.Events))-tableEvents.dropped)
		}
	}

	var errs error
	var retryEvents []publisher.Event
	var retryRows int64 = 0
	var droppedRows int64 = 0
	var successRows int64 = 0
	var successBytes int64 = 0

	for table, tableEvents := range tableEventsMap {
		if table == nilTable {
			errs = errors.Join(errs, tableEvents.err)
			droppedRows += tableEvents.dropped
			continue
		}

		if tableEvents.err != nil {
			errs = errors.Join(errs, tableEvents.err)
			retryRows += int64(len(tableEvents.Events))
			addBarrier(table, tableEvents)
			retryEvents = append(retryEvents, tableEvents.Events...)
			continue
		}

		droppedRows += tableEvents.dropped
		successRows += int64(len(tableEvents.Events)) - tableEvents.dropped
		successBytes += int64(len(tableEvents.serialization))
	}

	client.logger.Debugf("Stream-Load publish events: %d events have been published to doris in %v.", successRows, time.Since(begin))

	client.observer.Dropped(int(droppedRows))
	client.observer.Acked(int(successRows))
	client.observer.Failed(int(retryRows))

	client.reporter.IncrTotalBytes(successBytes)
	client.reporter.IncrTotalRows(successRows)

	return retryEvents, errs
}

const barrierKey = "__#BARRIER#__"

type barrierT struct {
	Table  string `json:"table"`
	Label  string `json:"label"`
	Length int    `json:"length"`
}

func addBarrier(table string, events *Events) {
	events.Events[len(events.Events)-1].Content.Fields[barrierKey] = &barrierT{
		Table:  table,
		Label:  events.Label,
		Length: len(events.Events),
	}
}

func getBarrierFromEvent(event *publisher.Event) (*barrierT, error) {
	value, err := event.Content.Fields.GetValue(barrierKey)
	if err != nil {
		return nil, err
	}
	barrier, ok := value.(*barrierT)
	if !ok {
		return nil, fmt.Errorf("invalid barrier event: %+v", event)
	}
	return barrier, nil
}

func removeBarrierFromEvent(event *publisher.Event) {
	_ = event.Content.Fields.Delete(barrierKey)
}

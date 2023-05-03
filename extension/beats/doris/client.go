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
    "net/http"
    "net/http/httputil"
    "strings"
    "time"

    "github.com/elastic/beats/v7/libbeat/beat"
    "github.com/elastic/beats/v7/libbeat/logp"
    "github.com/elastic/beats/v7/libbeat/outputs"
    "github.com/elastic/beats/v7/libbeat/outputs/codec"
    "github.com/elastic/beats/v7/libbeat/publisher"
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

    observer outputs.Observer
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

    Beat     beat.Info
    Codec    codec.Codec
    Observer outputs.Observer
    Logger   *logp.Logger
}

func (s clientSettings) String() string {
    return fmt.Sprintf("clientSettings{%s, %s, %s, %s}", s.URL, s.Timeout, s.LabelPrefix, s.Headers)
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

        codec:    s.Codec,
        observer: s.Observer,
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

    label := fmt.Sprintf("%s_%s_%s_%d", client.labelPrefix, client.database, client.table, time.Now().UnixMilli())
    rest, err := client.publishEvents(label, events)

    if len(rest) == 0 {
        batch.ACK()
        client.logger.Debugf("Success send events: %d", length)
    } else {
        client.observer.Failed(length)
        batch.RetryEvents(rest)
        client.logger.Warnf("Retry send events: %d", length)
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

    request.Header.Set("label", lable)
    for k, v := range client.headers {
        request.Header.Set(k, v)
    }
    response, responseErr := client.httpClient.Do(request)
    if responseErr != nil {
        client.logger.Errorf("Failed to stream-load request: %v", responseErr)
        return events, responseErr
    }

    defer response.Body.Close()
    if response.StatusCode < 200 || response.StatusCode >= 300 {
        client.logger.Errorf("Failed to stream-load request with status code %d",
            response.StatusCode)

        reqBytes, reqErr := httputil.DumpRequestOut(request, false)
        if reqErr != nil {
            client.logger.Errorf("Failed to dump stream-load request: %v", reqErr)
        } else {
            client.logger.Errorf("Stream-Load request dump:\n%s\n first event: %s",
                string(reqBytes), string(logFirstEvent))
        }

        respBytes, respErr := httputil.DumpResponse(response, true)
        if respErr != nil {
            client.logger.Errorf("Failed to dump stream-load response: %v", respErr)
        } else {
            client.logger.Errorf("Stream-Load response dump:\n%s", string(respBytes))
        }
        return events, nil
    }

    client.logger.Debugf("Stream-Load publish events: %d events have been published to doris in %v.",
        len(events)-dropped,
        time.Now().Sub(begin))

    client.observer.Dropped(dropped)
    client.observer.Acked(len(events) - dropped)
    return nil, nil
}

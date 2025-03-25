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
	"fmt"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

const logSelector = "doris"

func init() {
	outputs.RegisterType("doris", makeDoris)
}

func makeDoris(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	logger := logp.NewLogger(logSelector)

	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}
	if err := config.Validate(); err != nil {
		return outputs.Fail(err)
	}

	codecConfig := loadCodecConfig(config)
	codec, err := codec.CreateEncoder(beat, codecConfig)
	if err != nil {
		return outputs.Fail(err)
	}

	reporter := NewProgressReporter(config.LogProgressInterval, logger)

	clients := make([]outputs.NetworkClient, len(config.Hosts))
	for i, host := range config.Hosts {
		logger.Info("Making client for host: " + host)
		url, err := parseURL(host)
		if err != nil {
			return outputs.Fail(err)
		}

		streamLoadPath := fmt.Sprintf("/api/%s/%s/_stream_load", config.Database, config.Table)
		hostURL, err := common.MakeURL(url.Scheme, streamLoadPath, host, 8030)
		if err != nil {
			logger.Errorf("Invalid host param set: %s, Error: %+v", host, err)
			return outputs.Fail(err)
		}
		logger.Infof("Final http connection endpoint: %s", hostURL)

		var client outputs.NetworkClient
		client, err = NewDorisClient(clientSettings{
			URL:           hostURL,
			Timeout:       config.Timeout,
			Observer:      observer,
			Headers:       config.createHeaders(),
			Codec:         codec,
			LineDelimiter: config.LineDelimiter,
			LogRequest:    config.LogRequest,

			LabelPrefix: config.LabelPrefix,
			Database:    config.Database,
			Table:       config.Table,

			Reporter: reporter,
			Logger:   logger,
		})
		if err != nil {
			return outputs.Fail(err)
		}

		client = outputs.WithBackoff(client, config.Backoff.Init, config.Backoff.Max)
		clients[i] = client
	}
	retry := 0
	if config.MaxRetries < 0 {
		retry = -1
	} else {
		retry = config.MaxRetries
	}

	go reporter.Report()

	return outputs.SuccessNet(true, config.BulkMaxSize, retry, clients)
}

func loadCodecConfig(config config) codec.Config {
	if len(config.CodecFormatString) == 0 {
		return config.Codec
	}

	beatConfig := common.MustNewConfigFrom(map[string]interface{}{
		"format.string": config.CodecFormatString,
	})
	codecConfig := codec.Config{}
	if err := beatConfig.Unpack(&codecConfig); err != nil {
		panic(err)
	}
	return codecConfig
}

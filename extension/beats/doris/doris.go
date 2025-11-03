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
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
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

	// duplicate entries config.Worker times
	hosts := make([]string, 0, len(config.Hosts)*config.Worker)
	for _, entry := range config.Hosts {
		for i := 0; i < config.Worker; i++ {
			hosts = append(hosts, entry)
		}
	}
	clients := make([]outputs.NetworkClient, len(hosts))
	for i, host := range hosts {
		logger.Info("Making client for host: " + host)
		url, err := parseURL(host)
		if err != nil {
			return outputs.Fail(err)
		}

		urlPrefix, err := common.MakeURL(url.Scheme, "/api", host, 8030)
		if err != nil {
			logger.Errorf("Invalid host param set: %s, Error: %+v", host, err)
			return outputs.Fail(err)
		}
		logger.Infof("http connection endpoint: %s/%s/{table}/_stream_load", urlPrefix, config.Database)
		if len(config.Tables) > 0 {
			logger.Infof("tables: %+v", config.Tables)
		}
		if len(config.Table) > 0 {
			logger.Infof("table: %s", config.Table)
		}

		tableSelector, err := buildTableSelector(cfg)
		if err != nil {
			return outputs.Fail(err)
		}

		var client outputs.NetworkClient
		client, err = NewDorisClient(clientSettings{
			URLPrefix:     urlPrefix,
			Timeout:       config.Timeout,
			Observer:      observer,
			Headers:       config.createHeaders(),
			Codec:         codec,
			LineDelimiter: config.LineDelimiter,
			LogRequest:    config.LogRequest,

			LabelPrefix:   config.LabelPrefix,
			Database:      config.Database,
			TableSelector: tableSelector,
			DefaultTable:  config.DefaultTable,

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

type fmtSelector struct {
	Sel outil.Selector
}

func buildTableSelector(cfg *common.Config) (*fmtSelector, error) {
	selector, err := outil.BuildSelectorFromConfig(cfg, outil.Settings{
		Key:              "table",
		MultiKey:         "tables",
		EnableSingleOnly: true,
		FailEmpty:        true,
		Case:             outil.SelectorKeepCase,
	})

	if err != nil {
		return nil, err
	}

	return &fmtSelector{
		Sel: selector,
	}, nil
}

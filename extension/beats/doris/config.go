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
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type config struct {
	Hosts               []string `config:"fenodes"`
	HttpHosts           []string `config:"http_hosts"`
	User                string   `config:"user" validate:"required"`
	Password            string   `config:"password"`
	Database            string   `config:"database" validate:"required"`
	Table               string   `config:"table" validate:"required"`
	LabelPrefix         string   `config:"label_prefix"`
	LineDelimiter       string   `config:"line_delimiter"`
	LogRequest          bool     `config:"log_request"`
	LogProgressInterval int      `config:"log_progress_interval"`

	Headers map[string]string `config:"headers"`

	CodecFormatString string        `config:"codec_format_string"`
	Codec             codec.Config  `config:"codec"`
	Timeout           time.Duration `config:"timeout"`
	BulkMaxSize       int           `config:"bulk_max_size" validate:"min=1,nonzero"`
	MaxRetries        int           `config:"max_retries" validate:"min=-1,nonzero"`
	Backoff           backoff       `config:"backoff"`
}

type backoff struct {
	Init time.Duration `config:"init"`
	Max  time.Duration `config:"max"`
}

func defaultConfig() config {
	return config{
		Password:            "",
		LabelPrefix:         "beats",
		LineDelimiter:       "\n",
		LogRequest:          true,
		LogProgressInterval: 10,

		BulkMaxSize: 100000,
		MaxRetries:  -1,
		Backoff: backoff{
			Init: 1 * time.Second,
			Max:  60 * time.Second,
		},
	}
}

func (c *config) Validate() error {
	if len(c.HttpHosts) != 0 {
		c.Hosts = c.HttpHosts
	}
	if len(c.Hosts) == 0 {
		return errors.New("no http_hosts configured")
	}
	if len(c.Database) == 0 {
		return errors.New("no database configured")
	}
	if len(c.Table) == 0 {
		return errors.New("no table configured")
	}
	if len(c.CodecFormatString) == 0 && &c.Codec == nil {
		return errors.New("no codec_format_expression|codec configured")
	}
	return nil
}

func (c *config) createHeaders() map[string]string {
	headers := make(map[string]string)
	headers["Expect"] = "100-continue"
	headers["Content-Type"] = "text/plain;charset=utf-8"
	if len(c.User) != 0 {
		headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(c.User+":"+c.Password))
	}
	if len(c.LineDelimiter) != 0 && !strings.EqualFold(c.LineDelimiter, "\n") {
		headers["line_delimiter"] = c.LineDelimiter
	}

	if c.Headers != nil && len(c.Headers) != 0 {
		for k, v := range c.Headers {
			if strings.EqualFold("line_delimiter", k) {
				c.LineDelimiter = v
			}
			headers[k] = v
		}
	}
	return headers
}

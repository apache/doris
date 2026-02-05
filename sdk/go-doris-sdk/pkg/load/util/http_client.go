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

package util

import (
	"crypto/tls"
	"net/http"
	"sync"
	"time"
)

var (
	client *http.Client
	once   sync.Once
)

func GetHttpClient() *http.Client {
	once.Do(func() {
		client = buildHttpClient()
	})
	return client
}

func buildHttpClient() *http.Client {

	transport := &http.Transport{
		MaxIdleConnsPerHost: 30, // Maximum idle connections per host for connection reuse to reduce overhead
		MaxConnsPerHost:     50, // Maximum total connections (active + idle) per host, controls concurrency, excess will queue
		MaxIdleConns:        50, // Global maximum idle connections

		// TLS configuration for Doris HTTP endpoints
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Allow insecure connections for Doris HTTP endpoints
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second, // Total request timeout
	}

	return client
}

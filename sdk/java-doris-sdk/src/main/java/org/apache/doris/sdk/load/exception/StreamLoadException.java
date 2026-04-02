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

package org.apache.doris.sdk.load.exception;

/**
 * Thrown for retryable HTTP-level stream load failures (e.g. HTTP 5xx, connection errors).
 * Non-retryable business failures (bad data, auth error) are returned via LoadResponse.
 */
public class StreamLoadException extends RuntimeException {

    public StreamLoadException(String message) {
        super(message);
    }

    public StreamLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}

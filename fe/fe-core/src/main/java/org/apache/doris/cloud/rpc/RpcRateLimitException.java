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

package org.apache.doris.cloud.rpc;

import org.apache.doris.rpc.RpcException;

/**
 * Exception thrown when RPC rate limit is exceeded.
 * This exception is used to indicate that the rate limit for meta-service RPC calls
 * has been exceeded and the request cannot be processed.
 *
 * There are multiple scenarios for this exception:
 * 1. Too many waiting requests - when the waiting queue is full, requests are rejected immediately
 * 2. Wait timeout - when a request waits too long to acquire a rate limit permit
 * 3. Cost limit exceeded - when the cost limit is exceeded
 */
public class RpcRateLimitException extends RpcException {

    public RpcRateLimitException(String message) {
        this(message, null);
    }

    public RpcRateLimitException(String message, Exception cause) {
        super("", message, cause);
    }
}

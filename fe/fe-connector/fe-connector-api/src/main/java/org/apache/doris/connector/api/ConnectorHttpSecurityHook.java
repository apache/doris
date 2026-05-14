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

package org.apache.doris.connector.api;

/**
 * Security hook for connector HTTP requests.
 *
 * <p>Implementations perform URL validation (e.g., SSRF checks)
 * before connector code makes outbound HTTP requests. The hook
 * is provided by the engine via {@code ConnectorContext} and is
 * transparent to connector implementations.</p>
 *
 * <p>Usage pattern:</p>
 * <pre>
 *   hook.beforeRequest(url);
 *   try {
 *       // execute HTTP request
 *   } finally {
 *       hook.afterRequest();
 *   }
 * </pre>
 */
public interface ConnectorHttpSecurityHook {

    /**
     * Called before an outbound HTTP request.
     * Implementations may validate the URL and throw if it violates security policy.
     *
     * @param url the full URL being requested
     * @throws Exception if the URL is rejected by security policy
     */
    void beforeRequest(String url) throws Exception;

    /**
     * Called after an outbound HTTP request completes (success or failure).
     * Implementations should clean up any thread-local security state.
     */
    void afterRequest();

    /** A no-op hook that permits all requests. */
    ConnectorHttpSecurityHook NOOP = new ConnectorHttpSecurityHook() {
        @Override
        public void beforeRequest(String url) {
        }

        @Override
        public void afterRequest() {
        }
    };
}

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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.ConnectorHttpSecurityHook;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Runtime context provided by fe-core to connector implementations.
 * Provides access to engine-level services.
 */
public interface ConnectorContext {

    /** Returns the catalog name. */
    String getCatalogName();

    /** Returns the catalog ID. */
    long getCatalogId();

    /**
     * Returns engine-level environment properties that connectors may need.
     * These are system configurations from the FE, not catalog properties.
     *
     * <p>Known keys include:
     * <ul>
     *   <li>{@code doris_home} — the DORIS_HOME path</li>
     *   <li>{@code jdbc_drivers_dir} — the configured JDBC drivers directory</li>
     * </ul>
     */
    default Map<String, String> getEnvironment() {
        return Collections.emptyMap();
    }

    /**
     * Returns the HTTP security hook for SSRF protection.
     * Connectors making outbound HTTP requests should call this hook
     * before and after each request.
     */
    default ConnectorHttpSecurityHook getHttpSecurityHook() {
        return ConnectorHttpSecurityHook.NOOP;
    }

    /**
     * Sanitizes a JDBC URL according to engine-level security policies.
     * The engine may reject URLs that target internal networks, contain
     * banned parameters, or otherwise violate security rules.
     *
     * <p>Connectors MUST call this method before using any JDBC URL
     * to establish a database connection.
     *
     * @param jdbcUrl the raw JDBC URL
     * @return the sanitized URL (may be the same string if no changes needed)
     * @throws RuntimeException if the URL violates security policies
     */
    default String sanitizeJdbcUrl(String jdbcUrl) {
        return jdbcUrl;
    }

    /**
     * Executes a task within the catalog's authentication context.
     * For secured deployments (e.g., Kerberos), this wraps the call
     * with the appropriate UGI.doAs() or equivalent mechanism.
     *
     * <p>Connectors accessing secured external systems (e.g., Hive Metastore
     * with Kerberos) MUST use this method to wrap their external calls.</p>
     *
     * <p>The default implementation simply executes the task directly (simple auth).</p>
     *
     * @param task the task to execute within the authentication context
     * @param <T>  the return type of the task
     * @return the result of the task
     * @throws Exception if the task execution or authentication fails
     */
    default <T> T executeAuthenticated(Callable<T> task) throws Exception {
        return task.call();
    }
}

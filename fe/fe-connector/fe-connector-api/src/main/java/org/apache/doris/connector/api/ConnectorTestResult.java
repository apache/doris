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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Result of a connector connectivity test.
 *
 * <p>Connectors return this from {@link Connector#testConnection} to report
 * whether the data source is reachable. Individual sub-components (e.g.,
 * metastore, object storage) can report separate results via
 * {@link #getComponentResults()}.</p>
 */
public class ConnectorTestResult {

    private final boolean success;
    private final String message;
    private final Map<String, ConnectorTestResult> componentResults;

    private ConnectorTestResult(boolean success, String message,
            Map<String, ConnectorTestResult> componentResults) {
        this.success = success;
        this.message = message;
        this.componentResults = componentResults != null
                ? Collections.unmodifiableMap(componentResults)
                : Collections.emptyMap();
    }

    /** Creates a successful test result. */
    public static ConnectorTestResult success() {
        return new ConnectorTestResult(true, "OK", null);
    }

    /** Creates a successful test result with a message. */
    public static ConnectorTestResult success(String message) {
        return new ConnectorTestResult(true, message, null);
    }

    /** Creates a failed test result. */
    public static ConnectorTestResult failure(String message) {
        return new ConnectorTestResult(false, message, null);
    }

    /** Creates a test result with sub-component results. */
    public static ConnectorTestResult withComponents(
            Map<String, ConnectorTestResult> componentResults) {
        boolean allSuccess = componentResults.values().stream()
                .allMatch(ConnectorTestResult::isSuccess);
        return new ConnectorTestResult(allSuccess,
                allSuccess ? "All components OK" : "Some components failed",
                componentResults);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    /** Per-component results (e.g., "metastore" → OK, "storage" → FAIL). */
    public Map<String, ConnectorTestResult> getComponentResults() {
        return componentResults;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(success ? "SUCCESS" : "FAILURE").append(": ").append(message);
        if (!componentResults.isEmpty()) {
            sb.append(" [");
            componentResults.forEach((name, result) ->
                    sb.append(name).append("=").append(result.isSuccess() ? "OK" : "FAIL")
                            .append(", "));
            sb.setLength(sb.length() - 2);
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTestResult)) {
            return false;
        }
        ConnectorTestResult that = (ConnectorTestResult) o;
        return success == that.success && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, message);
    }
}

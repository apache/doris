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

import java.util.Objects;

/**
 * Result of a connector connectivity test.
 *
 * <p>Connectors return this from {@link Connector#testConnection} to report
 * whether the data source is reachable. A connector that probes several
 * sub-components (e.g. metastore plus object storage) reports the first
 * failure it hits, in {@link #getMessage()}.</p>
 */
public class ConnectorTestResult {

    private final boolean success;
    private final String message;

    private ConnectorTestResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    /** Creates a successful test result. */
    public static ConnectorTestResult success() {
        return new ConnectorTestResult(true, "OK");
    }

    /** Creates a successful test result with a message. */
    public static ConnectorTestResult success(String message) {
        return new ConnectorTestResult(true, message);
    }

    /** Creates a failed test result. */
    public static ConnectorTestResult failure(String message) {
        return new ConnectorTestResult(false, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return (success ? "SUCCESS" : "FAILURE") + ": " + message;
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

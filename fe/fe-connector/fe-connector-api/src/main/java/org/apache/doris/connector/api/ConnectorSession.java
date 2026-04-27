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

import java.util.Map;

/**
 * Session context passed to every connector operation.
 */
public interface ConnectorSession {

    /** Returns the unique query identifier. */
    String getQueryId();

    /** Returns the authenticated user name. */
    String getUser();

    /** Returns the session time zone identifier (e.g. "Asia/Shanghai"). */
    String getTimeZone();

    /** Returns the session locale (e.g. "en_US"). */
    String getLocale();

    /** Returns the catalog id. */
    long getCatalogId();

    /** Returns the catalog name this session is bound to. */
    String getCatalogName();

    /** Retrieves a typed session/catalog property. */
    <T> T getProperty(String name, Class<T> type);

    /** Returns all catalog-level configuration properties. */
    Map<String, String> getCatalogProperties();

    /**
     * Returns session-level variable overrides relevant to connector operations.
     *
     * <p>These are per-query settings from the user session (e.g., SET statements)
     * that connectors may need for planning decisions. Keys are the variable names
     * as defined in the FE session variable registry.</p>
     *
     * @return unmodifiable map of session variable name → string value; never null
     */
    default Map<String, String> getSessionProperties() {
        return java.util.Collections.emptyMap();
    }
}

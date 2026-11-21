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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** A statement's session: a fixed time zone and a real statement scope that records its loads. */
final class LanceTestSession implements ConnectorSession {
    private final String timeZone;
    private final Map<String, Object> memo = new HashMap<>();
    private int loads;

    LanceTestSession() {
        this("UTC");
    }

    LanceTestSession(String timeZone) {
        this.timeZone = timeZone;
    }

    /** How many times the statement scope ran a loader, which is how many metadata reads were made. */
    int loads() {
        return loads;
    }

    /** Puts a value where a loader would have, as if this statement had already read it. */
    void remember(String key, Object value) {
        memo.put(key, value);
    }

    @Override
    public ConnectorStatementScope getStatementScope() {
        return new ConnectorStatementScope() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> T computeIfAbsent(String key, Supplier<T> loader) {
                Object value = memo.get(key);
                if (value == null) {
                    loads++;
                    value = loader.get();
                    memo.put(key, value);
                }
                return (T) value;
            }
        };
    }

    @Override
    public String getQueryId() {
        return "lance-test-query";
    }

    @Override
    public String getUser() {
        return "root";
    }

    @Override
    public String getTimeZone() {
        return timeZone;
    }

    @Override
    public String getLocale() {
        return "en_US";
    }

    @Override
    public long getCatalogId() {
        return 1L;
    }

    @Override
    public String getCatalogName() {
        return "lance_test";
    }

    @Override
    public <T> T getProperty(String name, Class<T> type) {
        return null;
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return Collections.emptyMap();
    }
}

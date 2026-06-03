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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.ConnectorPushdownOps;
import org.apache.doris.connector.api.ConnectorSession;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link JdbcConnectorMetadata}.
 */
class JdbcConnectorMetadataTest {

    private ConnectorSession sessionWithProps(Map<String, String> props) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "test-query";
            }

            @Override
            public String getUser() {
                return "root";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0L;
            }

            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return props;
            }
        };
    }

    @Test
    void testSupportsCastPredicatePushdown_defaultTrue() {
        JdbcConnectorMetadata metadata = new JdbcConnectorMetadata(null, Collections.emptyMap());
        ConnectorSession session = sessionWithProps(Collections.emptyMap());
        Assertions.assertTrue(metadata.supportsCastPredicatePushdown(session));
    }

    @Test
    void testSupportsCastPredicatePushdown_explicitTrue() {
        JdbcConnectorMetadata metadata = new JdbcConnectorMetadata(null, Collections.emptyMap());
        Map<String, String> props = new HashMap<>();
        props.put("enable_jdbc_cast_predicate_push_down", "true");
        ConnectorSession session = sessionWithProps(props);
        Assertions.assertTrue(metadata.supportsCastPredicatePushdown(session));
    }

    @Test
    void testSupportsCastPredicatePushdown_explicitFalse() {
        JdbcConnectorMetadata metadata = new JdbcConnectorMetadata(null, Collections.emptyMap());
        Map<String, String> props = new HashMap<>();
        props.put("enable_jdbc_cast_predicate_push_down", "false");
        ConnectorSession session = sessionWithProps(props);
        Assertions.assertFalse(metadata.supportsCastPredicatePushdown(session));
    }

    @Test
    void testDefaultPushdownOps_alwaysTrue() {
        ConnectorPushdownOps defaultOps = new ConnectorPushdownOps() { };
        ConnectorSession session = sessionWithProps(Collections.emptyMap());
        Assertions.assertTrue(defaultOps.supportsCastPredicatePushdown(session));
    }
}

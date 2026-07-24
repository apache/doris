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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorPushdownOps;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.jdbc.client.JdbcConnectorClient;
import org.apache.doris.connector.jdbc.client.JdbcFieldInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Unit tests for {@link JdbcConnectorMetadata}.
 */
class JdbcConnectorMetadataTest {

    private ConnectorSession sessionWithProps(Map<String, String> props) {
        return sessionWithScope(props, ConnectorStatementScope.NONE);
    }

    private ConnectorSession sessionWithScope(Map<String, String> props, ConnectorStatementScope scope) {
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

            @Override
            public ConnectorStatementScope getStatementScope() {
                return scope;
            }
        };
    }

    /** Live per-statement scope (map-backed) so resolveInStatement memoizes within the statement. */
    private static ConnectorStatementScope liveScope() {
        return new ConnectorStatementScope() {
            private final Map<String, Object> arena = new HashMap<>();

            @Override
            @SuppressWarnings("unchecked")
            public <T> T computeIfAbsent(String key, Supplier<T> loader) {
                return (T) arena.computeIfAbsent(key, k -> loader.get());
            }
        };
    }

    /** Test double that counts the remote column fetches (getJdbcColumnsInfo round-trips). */
    private static final class CountingJdbcClient extends JdbcConnectorClient {
        private final AtomicInteger columnsFetches = new AtomicInteger();
        private final List<JdbcFieldInfo> fields;

        private CountingJdbcClient(List<JdbcFieldInfo> fields) {
            super("test_catalog", JdbcDbType.MYSQL, "jdbc:mysql://h:3306/test_db",
                    false, null, null, false, false);
            this.fields = fields;
        }

        @Override
        public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
            columnsFetches.incrementAndGet();
            return fields;
        }

        @Override
        public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
            return ConnectorType.of("INT");
        }
    }

    private static JdbcFieldInfo field(String name) {
        return new JdbcFieldInfo(name, Optional.empty(), 0,
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Like {@link CountingJdbcClient} but its type conversion MUTATES the field in place (allowNull=true) for
     * column "d", mirroring the real MySQL/ClickHouse overrides that run on the now-shared raw memo.
     */
    private static final class MutatingJdbcClient extends JdbcConnectorClient {
        private final AtomicInteger columnsFetches = new AtomicInteger();
        private final List<JdbcFieldInfo> fields;

        private MutatingJdbcClient(List<JdbcFieldInfo> fields) {
            super("test_catalog", JdbcDbType.MYSQL, "jdbc:mysql://h:3306/test_db",
                    false, null, null, false, false);
            this.fields = fields;
        }

        @Override
        public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
            columnsFetches.incrementAndGet();
            return fields;
        }

        @Override
        public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
            if ("d".equals(fieldInfo.getColumnName())) {
                fieldInfo.setAllowNull(true); // idempotent, mirrors the real convertDateToNull path
            }
            return ConnectorType.of("INT");
        }
    }

    private static List<String> columnSummary(ConnectorTableSchema schema) {
        List<String> out = new ArrayList<>();
        for (ConnectorColumn c : schema.getColumns()) {
            out.add(c.getName() + ":" + c.isNullable());
        }
        return out;
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

    @Test
    void columnHandlesAndSchemaShareOneRemoteFetchPerStatement() {
        // WHY (HP-1): within one statement the scan-path getColumnHandles (called ~2x per scan node) and a
        // schema-cache-miss getTableSchema each hit client.getJdbcColumnsInfo -- a remote
        // DatabaseMetaData.getColumns round-trip. Routing the raw fetch through the per-statement scope memo
        // (ConnectorStatementScopes.JDBC_COLUMNS, keyed by (catalogId, db, table, queryId)) collapses them to
        // ONE remote fetch. MUTATION: fetching directly on each call (pre-fix) -> counter 3 -> red.
        CountingJdbcClient client = new CountingJdbcClient(Arrays.asList(field("id"), field("name")));
        JdbcConnectorMetadata md = new JdbcConnectorMetadata(client, Collections.emptyMap());
        ConnectorSession session = sessionWithScope(Collections.emptyMap(), liveScope());
        JdbcTableHandle handle = new JdbcTableHandle("db", "t");

        Map<String, ConnectorColumnHandle> handles = md.getColumnHandles(session, handle);
        md.getColumnHandles(session, handle);
        md.getTableSchema(session, handle);

        Assertions.assertEquals(1, client.columnsFetches.get(),
                "getColumnHandles x2 + getTableSchema must share ONE remote column fetch per statement");
        Assertions.assertEquals(2, handles.size());
        Assertions.assertTrue(handles.containsKey("id") && handles.containsKey("name"),
                "the mapping is still applied per call on the shared raw fetch");
    }

    @Test
    void noneScopeFetchesColumnsEveryCall() {
        // Parity: under ConnectorStatementScope.NONE (offline / no live statement -- the default session scope),
        // the memo runs the loader on every call, byte-identical to the pre-fix always-fetch behavior.
        // MUTATION: memoizing under NONE -> counter 1 -> red.
        CountingJdbcClient client = new CountingJdbcClient(Collections.singletonList(field("id")));
        JdbcConnectorMetadata md = new JdbcConnectorMetadata(client, Collections.emptyMap());
        ConnectorSession session = sessionWithProps(Collections.emptyMap()); // default scope = NONE

        JdbcTableHandle handle = new JdbcTableHandle("db", "t");
        md.getColumnHandles(session, handle);
        md.getColumnHandles(session, handle);

        Assertions.assertEquals(2, client.columnsFetches.get(),
                "under NONE scope each call must fetch (no cross-call memo) -- parity with pre-fix");
    }

    @Test
    void getTableSchemaStableOverSharedMutatedMemoUnderMutatingTypeConversion() {
        // WHY: getTableSchema's jdbcTypeToConnectorType mutates the field in place (allowNull->true for some
        // date types); under the per-statement memo the raw list is SHARED across getTableSchema/getColumnHandles,
        // so a later getTableSchema reads an already-mutated field. Pin that the mutation is idempotent and
        // non-corrupting: two getTableSchema calls over the shared memo (with a getColumnHandles between, which
        // reads only names) yield byte-identical columns and reflect allowNull=true for the mutated column "d".
        // MUTATION: a non-idempotent field mutation, or getColumnHandles depending on the flag -> schemas diverge.
        MutatingJdbcClient client = new MutatingJdbcClient(Arrays.asList(field("d"), field("id")));
        JdbcConnectorMetadata md = new JdbcConnectorMetadata(client, Collections.emptyMap());
        ConnectorSession session = sessionWithScope(Collections.emptyMap(), liveScope());
        JdbcTableHandle handle = new JdbcTableHandle("db", "t");

        List<String> first = columnSummary(md.getTableSchema(session, handle));
        md.getColumnHandles(session, handle);
        List<String> second = columnSummary(md.getTableSchema(session, handle));

        Assertions.assertEquals(1, client.columnsFetches.get(), "one shared fetch across all three calls");
        Assertions.assertEquals(first, second, "getTableSchema over the shared (mutated) memo is stable");
        Assertions.assertTrue(first.contains("d:true"),
                "the mutating type conversion's idempotent allowNull=true is reflected in the schema");
    }
}

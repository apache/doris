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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.connector.api.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Guards the per-statement metaClient memos: hudi keeps TWO INDEPENDENT memos, one for the latest columns
 * (behind {@code getTableSchema} with no time-travel pin) and one for the latest completed instant (behind
 * {@code beginQuerySnapshot}). Within one statement each fact is read at most once; the two never couple. Each
 * assertion pins WHY it matters:
 * <ul>
 *   <li>repeated latest {@code getTableSchema} reads in one statement collapse to a single
 *       {@code getSchemaFromMetaClient} — a mutation dropping the schema memo makes it run twice and turns this red;</li>
 *   <li>repeated {@code beginQuerySnapshot} pins collapse to a single {@code latestInstant}, likewise;</li>
 *   <li>the two memos are INDEPENDENT — a schema read triggers NO instant read and vice versa (a mutation that
 *       coupled them, e.g. a shared loader, would make the cross-counter non-zero);</li>
 *   <li>under {@link ConnectorStatementScope#NONE} and a {@code null} session each fact loads every time —
 *       byte-identical to resolving it per call, as before the memo (no cross-statement caching).</li>
 * </ul>
 *
 * <p>The two loaders are overridden with canned results (no live metaClient / plugin auth), so this unit locks the
 * sharing + independence wiring; each loader's byte-parity with the real read is guaranteed by reusing the exact
 * connector method ({@code getSchemaFromMetaClient} / {@code latestInstant}) as the loader.</p>
 */
public class HudiStatementMemoTest {

    private static final long INSTANT = 20240101120000000L;

    /** Records how many times each latest-fact loader actually ran (a memo miss). */
    private static final class RecordingMetadata extends HudiConnectorMetadata {
        int schemaLoads;
        int instantLoads;

        RecordingMetadata() {
            super(null, Collections.emptyMap(), null);
        }

        @Override
        List<ConnectorColumn> getSchemaFromMetaClient(String basePath, String queryInstant) {
            this.schemaLoads++;
            return Collections.singletonList(
                    new ConnectorColumn("c", ConnectorType.of("INT"), "", true, null));
        }

        @Override
        long latestInstant(HudiTableHandle handle) {
            this.instantLoads++;
            return INSTANT;
        }
    }

    private static HudiTableHandle handle() {
        return new HudiTableHandle.Builder("db", "t", "s3://b/t", "COPY_ON_WRITE").build();
    }

    @Test
    public void repeatedLatestSchemaReadsShareOneLoadWithinStatement() {
        RecordingMetadata m = new RecordingMetadata();
        MemoSession session = new MemoSession(7L, "q1", new MemoScope());
        m.getTableSchema(session, handle());
        m.getTableSchema(session, handle());
        Assertions.assertEquals(1, m.schemaLoads, "repeated latest getTableSchema must share one metaClient read");
        Assertions.assertEquals(0, m.instantLoads, "a schema read must NOT trigger an instant read (memos independent)");
    }

    @Test
    public void repeatedSnapshotPinsShareOneLoadWithinStatement() {
        RecordingMetadata m = new RecordingMetadata();
        MemoSession session = new MemoSession(7L, "q1", new MemoScope());
        long a = m.beginQuerySnapshot(session, handle()).get().getSnapshotId();
        long b = m.beginQuerySnapshot(session, handle()).get().getSnapshotId();
        Assertions.assertEquals(INSTANT, a);
        Assertions.assertEquals(INSTANT, b);
        Assertions.assertEquals(1, m.instantLoads, "repeated beginQuerySnapshot must share one instant read");
        Assertions.assertEquals(0, m.schemaLoads, "an instant read must NOT trigger a schema read (memos independent)");
    }

    @Test
    public void noneScopeLoadsEachTime() {
        // A live session whose scope is NONE (no per-statement context) must NOT share — load every time.
        RecordingMetadata m = new RecordingMetadata();
        MemoSession session = new MemoSession(7L, "q1", ConnectorStatementScope.NONE);
        m.getTableSchema(session, handle());
        m.getTableSchema(session, handle());
        m.beginQuerySnapshot(session, handle());
        m.beginQuerySnapshot(session, handle());
        Assertions.assertEquals(2, m.schemaLoads, "NONE scope -> schema loads every time");
        Assertions.assertEquals(2, m.instantLoads, "NONE scope -> instant loads every time");
    }

    @Test
    public void nullSessionLoadsEachTime() {
        // Offline / direct-construction (null session): each resolve loads, byte-identical to loading every time.
        RecordingMetadata m = new RecordingMetadata();
        m.getTableSchema(null, handle());
        m.getTableSchema(null, handle());
        m.beginQuerySnapshot(null, handle());
        m.beginQuerySnapshot(null, handle());
        Assertions.assertEquals(2, m.schemaLoads, "null session -> schema loads every time");
        Assertions.assertEquals(2, m.instantLoads, "null session -> instant loads every time");
    }

    /** A statement scope that memoizes like the engine's real one (ConcurrentHashMap computeIfAbsent). */
    private static final class MemoScope implements ConnectorStatementScope {
        private final ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

        @Override
        @SuppressWarnings("unchecked")
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return (T) cache.computeIfAbsent(key, k -> loader.get());
        }
    }

    /** Minimal {@link ConnectorSession} carrying a catalog id, queryId and scope for the per-statement memo. */
    private static final class MemoSession implements ConnectorSession {
        private final long catalogId;
        private final String queryId;
        private final ConnectorStatementScope scope;

        MemoSession(long catalogId, String queryId, ConnectorStatementScope scope) {
            this.catalogId = catalogId;
            this.queryId = queryId;
            this.scope = scope;
        }

        @Override
        public long getCatalogId() {
            return catalogId;
        }

        @Override
        public String getQueryId() {
            return queryId;
        }

        @Override
        public String getSessionId() {
            return "session-" + queryId;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }

        @Override
        public String getUser() {
            return "u";
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
        public String getCatalogName() {
            return "c";
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
}

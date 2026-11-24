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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class NegativeNumericParameterTest {
    @Test
    public void testRejectNegativeSessionParameters() {
        // SET is sufficient. No table, rows, query execution, or large allocation is needed.
        // Each name has a consumer that requires a count, size, ratio, or elapsed time.
        SessionVariable session = new SessionVariable();
        for (String name : new String[] {
                SessionVariable.LOCAL_EXCHANGE_FREE_BLOCKS_LIMIT,
                SessionVariable.SPILL_STREAMING_AGG_MEM_LIMIT,
                SessionVariable.MAX_SCAN_MEM_RATIO,
                SessionVariable.DATA_QUEUE_MAX_BLOCKS,
                SessionVariable.PARTITION_TOPN_MAX_PARTITIONS,
                SessionVariable.PARTITION_TOPN_PER_PARTITION_ROWS,
                SessionVariable.CTE_MAX_RECURSION_DEPTH,
                SessionVariable.QUERY_CACHE_ENTRY_MAX_BYTES,
                SessionVariable.QUERY_CACHE_ENTRY_MAX_ROWS,
                SessionVariable.RUNTIME_FILTER_WAIT_TIME_MS,
                SessionVariable.MAX_INITIAL_FILE_SPLIT_SIZE,
                SessionVariable.MAX_FILE_SPLIT_SIZE,
                SessionVariable.RUNTIME_BLOOM_FILTER_SIZE,
                SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE,
                SessionVariable.RUNTIME_BLOOM_FILTER_MAX_SIZE,
                SessionVariable.INVERTED_INDEX_MAX_EXPANSIONS,
                SessionVariable.PARALLEL_SCAN_MIN_ROWS_PER_SCANNER,
                SessionVariable.SCAN_QUEUE_MEM_LIMIT,
                SessionVariable.SPILL_MIN_REVOCABLE_MEM,
                SessionVariable.SPILL_BUFFER_SIZE_BYTES,
                SessionVariable.SPILL_AGGREGATION_PARTITION_COUNT,
                SessionVariable.LOW_MEMORY_MODE_BUFFER_LIMIT,
                SessionVariable.SPILL_HASH_JOIN_PARTITION_COUNT,
                SessionVariable.SPILL_REPARTITION_MAX_DEPTH,
                SessionVariable.SPILL_JOIN_BUILD_SINK_MEM_LIMIT_BYTES,
                SessionVariable.SPILL_AGGREGATION_SINK_MEM_LIMIT_BYTES,
                SessionVariable.SPILL_SORT_SINK_MEM_LIMIT_BYTES,
                SessionVariable.SPILL_ANALYTIC_SINK_MEM_LIMIT_BYTES,
                SessionVariable.SPILL_SORT_MERGE_MEM_LIMIT_BYTES,
                SessionVariable.MAX_FETCH_REMOTE_TABLET_COUNT,
                SessionVariable.FETCH_REMOTE_SCHEMA_TIMEOUT_SECONDS,
                SessionVariable.HOT_VALUE_COLLECT_COUNT}) {
            DdlException error = Assertions.assertThrows(DdlException.class,
                    () -> set(session, SetType.SESSION, name, "-1"), name);
            Assertions.assertTrue(error.getMessage().contains("must be"), error.getMessage());
        }
    }

    @Test
    public void testRejectNegativeGlobalParametersBeforePersistence() {
        SessionVariable session = new SessionVariable();
        // Rejection must not require an edit log or change the shared global value.
        for (String name : new String[] {GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_BYTES,
                GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC, GlobalVariable.AUDIT_PLUGIN_MAX_SQL_LENGTH,
                GlobalVariable.AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH, GlobalVariable.PARTITION_ANALYZE_BATCH_SIZE,
                GlobalVariable.HUGE_PARTITION_LOWER_BOUND_ROWS, SessionVariable.HUGE_TABLE_DEFAULT_SAMPLE_ROWS}) {
            Assertions.assertThrows(DdlException.class, () -> set(session, SetType.GLOBAL, name, "-1"), name);
        }
        for (String value : new String[] {"-2", "0"}) {
            Assertions.assertThrows(DdlException.class,
                    () -> set(session, SetType.GLOBAL, GlobalVariable.AUDIT_PLUGIN_LOAD_TIMEOUT, value));
        }
        // -1 requests the existing stream-load default; do not reject this sentinel.
        Assertions.assertDoesNotThrow(() -> GlobalVariable.checkAuditPluginLoadTimeout("-1"));
    }

    @Test
    public void testPositiveCountsRejectZeroAndAuditTimeoutKeepsItsUpperBound() {
        SessionVariable session = new SessionVariable();
        // These counts require at least one item. Rejection happens before global persistence.
        Assertions.assertThrows(DdlException.class,
                () -> set(session, SetType.SESSION, SessionVariable.RUNTIME_BLOOM_FILTER_SIZE, "0"));
        for (String name : new String[] {GlobalVariable.PARTITION_ANALYZE_BATCH_SIZE,
                SessionVariable.HUGE_TABLE_DEFAULT_SAMPLE_ROWS}) {
            Assertions.assertThrows(DdlException.class, () -> set(session, SetType.GLOBAL, name, "0"));
        }
        int originalMaximum = Config.max_stream_load_timeout_second;
        try {
            Config.max_stream_load_timeout_second = 600;
            Assertions.assertDoesNotThrow(() -> GlobalVariable.checkAuditPluginLoadTimeout("600"));
            Assertions.assertThrows(DdlException.class,
                    () -> set(session, SetType.GLOBAL, GlobalVariable.AUDIT_PLUGIN_LOAD_TIMEOUT, "601"));
        } finally {
            Config.max_stream_load_timeout_second = originalMaximum;
        }
    }

    @Test
    public void testScanRatioBoundsKeepOriginalValue() throws Exception {
        SessionVariable session = new SessionVariable();
        set(session, SetType.SESSION, SessionVariable.MAX_SCAN_MEM_RATIO, "1");
        for (String value : new String[] {"-0.1", "1.1", "NaN", "1e309"}) {
            Assertions.assertThrows(DdlException.class,
                    () -> set(session, SetType.SESSION, SessionVariable.MAX_SCAN_MEM_RATIO, value));
            Assertions.assertEquals(1.0, session.maxScanMemRatio);
        }
    }

    @Test
    public void testFollowerKeepsReplayedGlobalValue() throws Exception {
        int original = GlobalVariable.auditPluginLoadTimeoutS;
        int originalMaximum = Config.max_stream_load_timeout_second;
        try {
            // The leader can have a different local limit. Its synchronized journal is authoritative.
            GlobalVariable.auditPluginLoadTimeoutS = 300000;
            Config.max_stream_load_timeout_second = 259200;
            SessionVariable session = new SessionVariable();
            VariableMgr.setVarForNonMasterFE(session, new SetVar(SetType.SESSION,
                    GlobalVariable.AUDIT_PLUGIN_LOAD_TIMEOUT, new StringLiteral("300000")));
            Assertions.assertEquals(300000, GlobalVariable.auditPluginLoadTimeoutS);
            // A rejected request has no new journal entry. It must not overwrite the last valid value.
            VariableMgr.setVarForNonMasterFE(session, new SetVar(SetType.SESSION,
                    GlobalVariable.AUDIT_PLUGIN_LOAD_TIMEOUT, new StringLiteral("-2")));
            Assertions.assertEquals(300000, GlobalVariable.auditPluginLoadTimeoutS);
        } finally {
            GlobalVariable.auditPluginLoadTimeoutS = original;
            Config.max_stream_load_timeout_second = originalMaximum;
        }
    }

    @Test
    public void testZeroAndNegativeSentinels() throws Exception {
        SessionVariable session = new SessionVariable();
        set(session, SetType.SESSION, SessionVariable.LOCAL_EXCHANGE_FREE_BLOCKS_LIMIT, "0");
        set(session, SetType.SESSION, SessionVariable.DATA_QUEUE_MAX_BLOCKS, "0");
        set(session, SetType.SESSION, SessionVariable.RUNTIME_BLOOM_FILTER_MIN_SIZE, "0");
        set(session, SetType.SESSION, SessionVariable.MAX_SCANNERS_CONCURRENCY, "-1");
        set(session, SetType.SESSION, SessionVariable.FILE_CACHE_QUERY_LIMIT_BYTES, "-2");
        // A full forwarded map contains legitimate defaults, including negative sentinels.
        Assertions.assertDoesNotThrow(() -> new SessionVariable()
                .setForwardedSessionVariables(session.getForwardVariables()));
    }

    @Test
    public void testForwardedNegativeValueIsReported() {
        SessionVariable session = new SessionVariable();
        // A request from an older FE must fail before it can execute with a negative count.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> session.setForwardedSessionVariables(Map.of("data_queue_max_blocks", "-1")));
    }

    private static void set(SessionVariable session, SetType type, String name, String value) throws DdlException {
        VariableMgr.setVar(session, new SetVar(type, name, new StringLiteral(value)));
    }
}

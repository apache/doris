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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FIX-E (explain gap) — pins the connector half of the re-emitted EXPLAIN lines the legacy
 * {@code PaimonScanNode} produced but the SPI scan path dropped:
 * {@code paimonNativeReadSplits=<native>/<total>} ({@link PaimonScanPlanProvider#appendExplainInfo})
 * and the deletion-file lookup behind {@code deleteFileNum}
 * ({@link PaimonScanPlanProvider#getDeleteFiles}), plus the two {@link PaimonScanRange} getters that
 * feed the generic node's accounting.
 *
 * <p>Offline: these methods touch neither the catalog nor a live table, so a 2-arg provider with a
 * {@code null} catalogOps is sufficient.</p>
 */
public class PaimonScanExplainTest {

    private static PaimonScanPlanProvider provider() {
        return new PaimonScanPlanProvider(new HashMap<>(), null);
    }

    // ==================== appendExplainInfo: paimonNativeReadSplits ====================

    @Test
    public void appendExplainInfoEmitsNativeReadSplitsFromSyntheticKeys() {
        // WHY: under force_jni_scanner=true the node accumulates 0 native of 1 total and injects them
        // as the synthetic __native_read_splits / __total_read_splits keys; the provider must emit
        // exactly "paimonNativeReadSplits=0/1" (the assertion in test_paimon_catalog_varbinary /
        // _timestamp_tz). MUTATION: dropping the override, or reading the wrong keys, makes this red.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "0");
        props.put("__total_read_splits", "1");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "  ", props);

        Assertions.assertEquals("  paimonNativeReadSplits=0/1\n", out.toString());
    }

    @Test
    public void appendExplainInfoEmitsNonZeroNativeOverTotal() {
        // A mixed native/JNI scan: 3 native of 5 total -> "paimonNativeReadSplits=3/5". Pins that the
        // raw counts pass through verbatim (numerator native, denominator total), not a recomputation.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "3");
        props.put("__total_read_splits", "5");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "", props);

        Assertions.assertEquals("paimonNativeReadSplits=3/5\n", out.toString());
    }

    @Test
    public void appendExplainInfoSkipsWhenSyntheticKeysAbsent() {
        // WHY: when the node has not yet accounted splits (or another connector's props map), the keys
        // are absent and the line must NOT print (never a spurious "0/0"). Pins the null-guard.
        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "  ", new HashMap<>());
        Assertions.assertEquals("", out.toString());
    }

    // ==================== appendExplainInfo: PaimonSplitStats block (VERBOSE) ====================

    @Test
    public void appendExplainInfoEmitsJniSplitStatsBlockWhenVerbose() {
        // WHY: the legacy PaimonScanNode emitted a per-split "PaimonSplitStats:" block under VERBOSE;
        // the SPI path dropped it, breaking paimon_data_system_table's assertJniPath
        // (contains "SplitStat [type=JNI"). Under force_jni_scanner every range is JNI: 0 native of 2
        // total -> two JNI SplitStat lines. MUTATION: dropping the block, or mistyping the lines as
        // NATIVE, makes this red.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "0");
        props.put("__total_read_splits", "2");
        props.put("__explain_verbose", "true");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "", props);

        Assertions.assertEquals(
                "paimonNativeReadSplits=0/2\n"
                        + "PaimonSplitStats: \n"
                        + "  SplitStat [type=JNI]\n"
                        + "  SplitStat [type=JNI]\n",
                out.toString());
    }

    @Test
    public void appendExplainInfoOmitsSplitStatsBlockWhenNotVerbose() {
        // WHY: legacy gated the block on VERBOSE; a plain (non-verbose) EXPLAIN must show only the
        // paimonNativeReadSplits line, never the per-split block. Pins the verbose gate (no
        // __explain_verbose key -> no block). MUTATION: emitting the block unconditionally is killed.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "0");
        props.put("__total_read_splits", "2");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "", props);

        Assertions.assertEquals("paimonNativeReadSplits=0/2\n", out.toString());
    }

    @Test
    public void appendExplainInfoEmitsBothTypesForMixedNativeJniScan() {
        // A mixed scan: 1 native of 2 total -> one NATIVE and one JNI SplitStat line (assertNativePath
        // checks "SplitStat [type=NATIVE", assertJniPath checks "SplitStat [type=JNI"). Pins that the
        // native numerator splits the lines NATIVE-first.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "1");
        props.put("__total_read_splits", "2");
        props.put("__explain_verbose", "true");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "", props);

        String text = out.toString();
        Assertions.assertTrue(text.contains("SplitStat [type=NATIVE]"), text);
        Assertions.assertTrue(text.contains("SplitStat [type=JNI]"), text);
    }

    @Test
    public void appendExplainInfoTruncatesSplitStatsBeyondFour() {
        // Legacy truncation parity: > 4 splits -> first 3 + "... other N paimon split stats ..." + last.
        // 0 native of 6 -> "... other 2 paimon split stats ...". Pins the truncation so VERBOSE output
        // stays bounded for large scans.
        Map<String, String> props = new HashMap<>();
        props.put("__native_read_splits", "0");
        props.put("__total_read_splits", "6");
        props.put("__explain_verbose", "true");

        StringBuilder out = new StringBuilder();
        provider().appendExplainInfo(out, "", props);

        String text = out.toString();
        Assertions.assertTrue(text.contains("... other 2 paimon split stats ..."), text);
        // first 3 + truncation marker + last = 5 SplitStat/marker rows, not 6 raw lines
        Assertions.assertEquals(4, text.split("SplitStat \\[type=").length - 1, text);
    }

    // ==================== getDeleteFiles: deletion-vector path ====================

    /** Builds a real per-range thrift carrying a paimon deletion file at {@code path}. */
    private static TTableFormatFileDesc rangeWithDeletionFile(String path) {
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("orc")
                .deletionFile(path, 8L, 16L)        // native path: no paimon.split, with a deletion file
                .build();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc tableFormat = new TTableFormatFileDesc();
        range.populateRangeParams(tableFormat, rangeDesc);
        return tableFormat;
    }

    @Test
    public void getDeleteFilesReturnsDeletionPath() {
        // WHY: the VERBOSE block counts deleteFileNum from this list; it must surface the deletion-vector
        // path threaded onto the range's TPaimonFileDesc. MUTATION: returning empty (no read of
        // getDeletionFile().getPath()) regresses deleteFileNum to 0.
        TTableFormatFileDesc tableFormat = rangeWithDeletionFile("oss://bkt/db/tbl/index/dv-1.bin");

        List<String> files = provider().getDeleteFiles(tableFormat);

        Assertions.assertEquals(1, files.size());
        Assertions.assertEquals("oss://bkt/db/tbl/index/dv-1.bin", files.get(0));
    }

    @Test
    public void getDeleteFilesEmptyWhenNoDeletionFile() {
        // A native range with NO deletion file -> empty list (deleteFileNum contribution 0). Pins the
        // isSetDeletionFile guard, mirroring legacy PaimonScanNode.getDeleteFiles.
        PaimonScanRange range = new PaimonScanRange.Builder().fileFormat("orc").build();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        TTableFormatFileDesc tableFormat = new TTableFormatFileDesc();
        range.populateRangeParams(tableFormat, rangeDesc);

        Assertions.assertTrue(provider().getDeleteFiles(tableFormat).isEmpty());
    }

    @Test
    public void getDeleteFilesEmptyWhenNoPaimonParams() {
        // A bare table-format desc (no paimon params) -> empty, never NPE. Pins the isSetPaimonParams
        // guard so the VERBOSE loop is safe for any range shape.
        Assertions.assertTrue(provider().getDeleteFiles(new TTableFormatFileDesc()).isEmpty());
        Assertions.assertTrue(provider().getDeleteFiles(null).isEmpty());
    }

    // ==================== PaimonScanRange getter permutations ====================

    @Test
    public void nativeRangeIsNativeAndCarriesNoCount() {
        // A native range: a path, no paimon.split, no row count -> isNativeReadRange()=true,
        // getPushDownRowCount()=-1. This is the range that increments the native numerator.
        PaimonScanRange range = new PaimonScanRange.Builder()
                .path("oss://bkt/db/tbl/data-1.orc")
                .fileFormat("orc")
                .build();
        Assertions.assertTrue(range.isNativeReadRange());
        Assertions.assertEquals(-1, range.getPushDownRowCount());
    }

    @Test
    public void jniRangeIsNotNative() {
        // A JNI range carries paimon.split (and no native path) -> NOT native. Under force_jni_scanner
        // every range is this shape, so the native numerator stays 0 (the 0 in 0/1).
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .paimonSplit("serialized-split")
                .tableLocation("oss://bkt/db/tbl")
                .build();
        Assertions.assertFalse(range.isNativeReadRange());
    }

    @Test
    public void countRangeCarriesRowCountAndIsNotNative() {
        // The collapsed COUNT(*) range: a JNI split carrying paimon.row_count -> NOT native, and
        // getPushDownRowCount() returns the summed total (12). This is what feeds "pushdown agg=COUNT
        // (12)". MUTATION: a getter ignoring paimon.row_count would return -1 here.
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .paimonSplit("serialized-split")
                .tableLocation("oss://bkt/db/tbl")
                .rowCount(12L)
                .build();
        Assertions.assertFalse(range.isNativeReadRange());
        Assertions.assertEquals(12L, range.getPushDownRowCount());
    }
}

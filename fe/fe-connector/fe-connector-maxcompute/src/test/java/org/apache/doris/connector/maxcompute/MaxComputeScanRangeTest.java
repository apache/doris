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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * FIX-READ-SPLIT (P4-T06d) — guards the BYTE_SIZE split-size sentinel produced by
 * {@link MaxComputeScanPlanProvider}'s byte_size branch.
 *
 * <p>WHY this matters: BE has no {@code split_type} field on the wire — it classifies a
 * MaxCompute split purely by the numeric {@code split_size} it receives. {@code MaxComputeJniScanner}
 * does {@code if (splitSize == -1) BYTE_SIZE else ROW_OFFSET} (MaxComputeJniScanner.java:125-128),
 * then in {@code open()} builds {@code IndexedInputSplit} (BYTE_SIZE) or
 * {@code RowRangeInputSplit(sessionId, startOffset, splitSize)} (ROW_OFFSET). If a byte_size split
 * carries a real byte count (e.g. 268435456) instead of {@code -1}, BE silently mis-reads it as a
 * ROW_OFFSET split and returns CORRUPT data (no error). So the provider's byte_size branch MUST emit
 * size {@code -1}; this mirrors legacy {@code MaxComputeScanNode}'s
 * {@code MaxComputeSplit(..., length=-1, fileLength=splitByteSize, ...)}.</p>
 *
 * <p>This test drives the PROVIDER's real byte_size split-building code
 * ({@code buildSplitsFromSession}) with offline fakes (no network, no live ODPS) — so it locks the
 * provider's CHOICE of {@code -1}, not merely the range mechanism. Reverting the byte_size branch to
 * {@code .length(splitByteSize)} makes {@code byteSizeBranchEmitsMinusOneSizeSentinel} FAIL
 * (getSize() would become the real byte size). The row_offset case is the contrast that proves only
 * byte_size uses the sentinel — its size is the real row count, never {@code -1}.</p>
 *
 * <p>The connector module has no fe-core / Mockito; we reach the private split-building method via
 * reflection and stub the ODPS {@code TableBatchReadSession} / {@code InputSplitAssigner} with plain
 * Serializable fakes ({@code serializeSession} writes the session, so it must be Serializable).</p>
 */
public class MaxComputeScanRangeTest {

    private static final long SPLIT_BYTE_SIZE = 268435456L; // ODPS default byte-size split

    @Test
    public void byteSizeBranchEmitsMinusOneSizeSentinel() throws Exception {
        // Build via the provider's REAL byte_size branch.
        ConnectorScanRange range = buildSingleRange(
                MCConnectorProperties.SPLIT_BY_BYTE_SIZE_STRATEGY,
                new FakeSession(new FakeAssigner(SplitKind.BYTE_SIZE)));

        TFileRangeDesc rangeDesc = populate(range);

        // The whole point of the fix: BE distinguishes BYTE_SIZE from ROW_OFFSET by size == -1.
        // If the provider reverts to .length(splitByteSize) this assertion fails with 268435456,
        // which is exactly the corrupt-read bug (BE would treat it as ROW_OFFSET row count).
        Assertions.assertEquals(-1L, rangeDesc.getSize(),
                "byte_size split must carry size == -1 sentinel; any real byte count makes BE "
                        + "mis-classify it as ROW_OFFSET and read corrupt data");
        // start is the split index (set by the byte_size branch), unaffected by the sentinel.
        Assertions.assertEquals(7L, rangeDesc.getStartOffset(),
                "byte_size split start must be the IndexedInputSplit splitIndex");
        // path mirrors legacy "[ splitIndex , -1 ]".
        Assertions.assertEquals("[ 7 , -1 ]", rangeDesc.getPath(),
                "byte_size split path must mirror legacy '[ splitIndex , -1 ]'");
    }

    @Test
    public void rowOffsetBranchKeepsRealRowCount() throws Exception {
        // Contrast: the row_offset branch must NOT use the sentinel; it sends the real row count
        // so BE builds RowRangeInputSplit(sessionId, startOffset, splitSize). This locks the intent
        // that ONLY byte_size uses -1 — guarding against an over-broad "set everything to -1" fix.
        ConnectorScanRange range = buildSingleRange(
                MCConnectorProperties.SPLIT_BY_ROW_COUNT_STRATEGY,
                new FakeSession(new FakeAssigner(SplitKind.ROW_OFFSET)));

        TFileRangeDesc rangeDesc = populate(range);

        Assertions.assertEquals(FakeAssigner.ROW_COUNT, rangeDesc.getSize(),
                "row_offset split must carry the real row count (BE reads it as RowRangeInputSplit "
                        + "size), never the -1 byte_size sentinel");
    }

    /**
     * Invokes the provider's private {@code buildSplitsFromSession} (which contains the byte_size /
     * row_offset branches under test) with a stubbed session, returning the single produced range.
     */
    private static ConnectorScanRange buildSingleRange(String strategy, TableBatchReadSession session)
            throws Exception {
        MaxComputeScanPlanProvider provider = newUninitializedProvider();
        setField(provider, "splitStrategy", strategy);
        setField(provider, "splitByteSize", SPLIT_BYTE_SIZE);
        setField(provider, "splitRowCount", FakeAssigner.ROW_COUNT);
        setField(provider, "readTimeout", 120);
        setField(provider, "connectTimeout", 10);
        setField(provider, "retryTimes", 4);

        Method m = MaxComputeScanPlanProvider.class.getDeclaredMethod(
                "buildSplitsFromSession", TableBatchReadSession.class, com.aliyun.odps.Table.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ConnectorScanRange> ranges =
                (List<ConnectorScanRange>) m.invoke(provider, session, null);
        Assertions.assertEquals(1, ranges.size(), "fake assigner yields exactly one split");
        return ranges.get(0);
    }

    /** Constructs the provider without running ctor logic / property init (we set fields directly). */
    private static MaxComputeScanPlanProvider newUninitializedProvider() throws Exception {
        // The ctor only stores the connector reference; buildSplitsFromSession never touches it.
        return new MaxComputeScanPlanProvider(null);
    }

    private static TFileRangeDesc populate(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);
        return rangeDesc;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = MaxComputeScanPlanProvider.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private enum SplitKind { BYTE_SIZE, ROW_OFFSET }

    /** Serializable stub session — {@code serializeSession} writes it, so it must serialize. */
    private static final class FakeSession implements TableBatchReadSession {
        private static final long serialVersionUID = 1L;
        private final transient InputSplitAssigner assigner;

        FakeSession(InputSplitAssigner assigner) {
            this.assigner = assigner;
        }

        // The only method the split-building path under test actually calls.
        @Override
        public InputSplitAssigner getInputSplitAssigner() {
            return assigner;
        }

        // Remaining abstract methods are never reached at plan time (read/reader paths only).
        @Override
        public DataSchema readSchema() {
            throw new UnsupportedOperationException("not used in plan-time test");
        }

        @Override
        public boolean supportsDataFormat(DataFormat dataFormat) {
            throw new UnsupportedOperationException("not used in plan-time test");
        }

        @Override
        public String toJson() {
            throw new UnsupportedOperationException("not used in plan-time test");
        }

        @Override
        public String getId() {
            return FakeAssigner.SESSION_ID;
        }

        @Override
        public TableIdentifier getTableIdentifier() {
            throw new UnsupportedOperationException("not used in plan-time test");
        }

        @Override
        public SessionStatus getStatus() {
            throw new UnsupportedOperationException("not used in plan-time test");
        }
    }

    /** Stub assigner producing one split of the requested kind. */
    private static final class FakeAssigner implements InputSplitAssigner {
        private static final long serialVersionUID = 1L;
        static final String SESSION_ID = "fake-session";
        static final long ROW_COUNT = 1000L;
        private final SplitKind kind;

        FakeAssigner(SplitKind kind) {
            this.kind = kind;
        }

        @Override
        public int getSplitsCount() {
            return 1;
        }

        @Override
        public InputSplit[] getAllSplits() {
            // BYTE_SIZE branch casts to IndexedInputSplit and reads getSplitIndex().
            return new InputSplit[] {new IndexedInputSplit(SESSION_ID, 7)};
        }

        @Override
        public long getTotalRowCount() {
            return ROW_COUNT; // one split: offset 0, count ROW_COUNT
        }

        @Override
        public InputSplit getSplitByRowOffset(long offset, long count) {
            return new IndexedInputSplit(SESSION_ID, (int) offset);
        }
    }
}

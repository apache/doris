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

package org.apache.doris.planner;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TSortInfo;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Plan-provider mode tests for {@link PluginDrivenTableSink} (W-phase W5).
 *
 * <p>The connector supplies a {@link ConnectorWritePlanProvider}; the sink
 * delegates {@link PluginDrivenTableSink#bindDataSink} to
 * {@link ConnectorWritePlanProvider#planWrite} and adopts the connector-built
 * opaque {@link TDataSink} verbatim, passing a {@link ConnectorWriteHandle} that
 * carries the bound target table handle and write columns. This is the single,
 * source-agnostic write path: every write-capable connector (jdbc / maxcompute /
 * iceberg) produces its own {@code T*TableSink} this way.</p>
 */
public class PluginDrivenTableSinkTest {

    /** Hand-written {@link ConnectorWritePlanProvider} double recording the delegated call. */
    private static final class RecordingWritePlanProvider implements ConnectorWritePlanProvider {
        private final ConnectorSinkPlan plan;
        private ConnectorSession seenSession;
        private ConnectorWriteHandle seenHandle;
        private ConnectorWriteHandle seenExplainHandle;

        private RecordingWritePlanProvider(ConnectorSinkPlan plan) {
            this.plan = plan;
        }

        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            this.seenSession = session;
            this.seenHandle = handle;
            return plan;
        }

        @Override
        public void appendExplainInfo(StringBuilder output, String prefix,
                ConnectorSession session, ConnectorWriteHandle handle) {
            this.seenExplainHandle = handle;
        }
    }

    @Test
    public void bindDataSinkDelegatesToWritePlanProvider() throws AnalysisException {
        TDataSink expected = new TDataSink(TDataSinkType.MAXCOMPUTE_TABLE_SINK);
        RecordingWritePlanProvider provider =
                new RecordingWritePlanProvider(new ConnectorSinkPlan(expected));
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() { };
        List<ConnectorColumn> columns = new ArrayList<>();

        // targetTable is null: the plan-provider path never dereferences it (the connector
        // resolves table facts from its own tableHandle), so a unit of the delegation needs
        // no full PluginDrivenExternalTable.
        PluginDrivenTableSink sink =
                new PluginDrivenTableSink(null, provider, null, tableHandle, columns);
        sink.bindDataSink(Optional.empty());

        // The connector-built opaque sink is adopted verbatim.
        Assert.assertSame(expected, sink.toThrift());
        // The bound facts reach the connector through the write handle.
        Assert.assertNotNull(provider.seenHandle);
        Assert.assertSame(tableHandle, provider.seenHandle.getTableHandle());
        Assert.assertSame(columns, provider.seenHandle.getColumns());
        Assert.assertFalse(provider.seenHandle.isOverwrite());
        Assert.assertTrue(provider.seenHandle.getStaticPartitionSpec().isEmpty());
        // No engine-built write sort by default -> the handle carries no sort info.
        Assert.assertNull(provider.seenHandle.getSortInfo());
    }

    @Test
    public void bindDataSinkKeepsWriteSubsetSeparateFromBoundTargetSchema() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK)));
        ConnectorColumn id = Mockito.mock(ConnectorColumn.class);
        ConnectorColumn name = Mockito.mock(ConnectorColumn.class);
        List<ConnectorColumn> writeColumns = Collections.singletonList(id);
        List<ConnectorColumn> boundTargetColumns = java.util.Arrays.asList(id, name);

        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, writeColumns,
                boundTargetColumns, null, WriteOperation.INSERT, false);
        sink.bindDataSink(Optional.empty());

        Assert.assertSame(writeColumns, provider.seenHandle.getColumns());
        Assert.assertEquals(boundTargetColumns, provider.seenHandle.getBoundTargetColumns());
        Assert.assertNotSame(boundTargetColumns, provider.seenHandle.getBoundTargetColumns());
    }

    @Test
    public void bindDataSinkThreadsEngineBuiltWriteSortInfoToHandle() throws AnalysisException {
        // WHY: the connector's planWrite cannot build a TSortInfo (the bound output exprs live only in the
        // engine). For a connector that declares write-sort columns (iceberg WRITE ORDERED BY), the engine
        // builds the TSortInfo and hands it to this sink, which must thread it onto the write handle so the
        // connector can stamp it on its opaque sink. Without this, a sorted iceberg table writes unsorted.
        TSortInfo engineBuilt = new TSortInfo();
        engineBuilt.setIsAscOrder(Collections.singletonList(true));
        engineBuilt.setNullsFirst(Collections.singletonList(true));
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK)));

        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(), engineBuilt);
        sink.bindDataSink(Optional.empty());

        Assert.assertSame(engineBuilt, provider.seenHandle.getSortInfo());
    }

    @Test
    public void bindDataSinkThreadsBoundWriteMetadataIdentityToHandle() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK)));
        String metadataIdentity = "sort-generation-3/spec-generation-7";

        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                Collections.emptyList(), null, WriteOperation.INSERT, false, metadataIdentity);
        sink.bindDataSink(Optional.empty());

        Assert.assertEquals(metadataIdentity, provider.seenHandle.getBoundWriteMetadataIdentity());
    }

    @Test
    public void bindDataSinkThreadsBranchNameToHandle() throws AnalysisException {
        // WHY: INSERT INTO t@branch carries the target branch on the PluginDrivenInsertCommandContext;
        // bindDataSink must thread it onto the write handle so a versioned-table connector (iceberg)
        // points the commit at the branch. Without this, the branch is silently dropped and the write
        // lands on the table's default ref. MUTATION: dropping `branchName = ctx.getBranchName()` ->
        // handle carries Optional.empty() -> assertion red.
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK)));
        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setBranchName(Optional.of("br_1"));

        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>());
        sink.bindDataSink(Optional.of(ctx));

        Assert.assertEquals(Optional.of("br_1"), provider.seenHandle.getBranchName());
    }

    @Test
    public void getExplainStringDelegatesConnectorWriteDetail() {
        PluginDrivenExternalTable targetTable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(targetTable.getName()).thenReturn("t1");
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() { };

        // A provider that contributes a connector-specific EXPLAIN line via the write hook.
        ConnectorWritePlanProvider provider = new ConnectorWritePlanProvider() {
            @Override
            public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
                return new ConnectorSinkPlan(new TDataSink(TDataSinkType.JDBC_TABLE_SINK));
            }

            @Override
            public void appendExplainInfo(StringBuilder output, String prefix,
                    ConnectorSession session, ConnectorWriteHandle handle) {
                output.append(prefix).append("  INSERT SQL: SELECT 1\n");
            }
        };

        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                targetTable, provider, null, tableHandle, new ArrayList<>());

        String explain = sink.getExplainString("", TExplainLevel.NORMAL);
        Assert.assertTrue(explain, explain.contains("PLUGIN-DRIVEN TABLE SINK"));
        Assert.assertTrue(explain, explain.contains("TABLE: t1"));
        // The source-agnostic sink delegates connector-specific detail through appendExplainInfo.
        Assert.assertTrue(explain, explain.contains("INSERT SQL: SELECT 1"));

        // BRIEF short-circuits before any connector detail.
        String brief = sink.getExplainString("", TExplainLevel.BRIEF);
        Assert.assertFalse(brief, brief.contains("INSERT SQL"));
    }

    @Test
    public void bindDataSinkDefaultsWriteOperationToInsert() throws AnalysisException {
        // WHY: a plain INSERT sink (the 5-arg / 6-arg ctors) must keep WriteOperation.INSERT on the write
        // handle so the connector's planWrite stays on the byte-identical INSERT path (TIcebergTableSink,
        // promoted to OVERWRITE only via isOverwrite()). This pins the dormant-default that guarantees
        // pre-flip parity for every existing write-capable connector (jdbc / maxcompute / iceberg INSERT).
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.MAXCOMPUTE_TABLE_SINK)));
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>());
        sink.bindDataSink(Optional.empty());

        Assert.assertEquals(WriteOperation.INSERT, provider.seenHandle.getWriteOperation());
    }

    @Test
    public void bindDataSinkThreadsMergeWriteOperationToHandle() throws AnalysisException {
        // WHY: a post-flip MERGE INTO / UPDATE on an SPI iceberg table builds this sink with
        // WriteOperation.MERGE; bindDataSink must thread it onto the write handle so the connector's
        // planWrite dispatches to TIcebergMergeSink (RowDelta at commit) instead of the INSERT
        // TIcebergTableSink. Without the handle's getWriteOperation() override, the op is silently lost
        // and a MERGE would write as an append. MUTATION: thread INSERT here -> assertion red.
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK)));
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                null, WriteOperation.MERGE);
        sink.bindDataSink(Optional.empty());

        Assert.assertEquals(WriteOperation.MERGE, provider.seenHandle.getWriteOperation());
    }

    @Test
    public void bindDataSinkThreadsDeleteWriteOperationToHandle() throws AnalysisException {
        // WHY: a post-flip DELETE on an SPI iceberg table builds this sink with WriteOperation.DELETE;
        // bindDataSink must thread it so planWrite dispatches to TIcebergDeleteSink. Same loss-of-op
        // hazard as MERGE. MUTATION: thread INSERT here -> assertion red.
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_DELETE_SINK)));
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                null, WriteOperation.DELETE);
        sink.bindDataSink(Optional.empty());

        Assert.assertEquals(WriteOperation.DELETE, provider.seenHandle.getWriteOperation());
    }

    @Test
    public void bindDataSinkThreadsDeleteOnlyMergeToHandle() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK)));
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                null, WriteOperation.MERGE, false, true);
        sink.bindDataSink(Optional.empty());

        // Delete-only MERGE must bypass data-file validation while retaining cardinality enforcement.
        Assert.assertFalse(provider.seenHandle.isWritesDataFiles());
        Assert.assertTrue(provider.seenHandle.isRequireMergeCardinalityCheck());
    }

    @Test
    public void deleteOnlyMergeVersionFenceAppliesOnlyToVariantSchemas() throws AnalysisException {
        int original = Config.be_exec_version;
        try {
            Config.be_exec_version = 11;
            RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                    new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK)));
            ConnectorColumn id = new ConnectorColumn(
                    "id", ConnectorType.of("INT"), null, true, null);
            PluginDrivenTableSink nonVariantSink = new PluginDrivenTableSink(
                    null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                    Collections.singletonList(id), null, WriteOperation.MERGE, false, true, null);

            nonVariantSink.bindDataSink(Optional.empty());
            Assert.assertNotNull(provider.seenHandle);

            ConnectorColumn payload = new ConnectorColumn(
                    "payload", ConnectorType.of("VARIANT_COMPUTE_V2"), null, true, null);
            PluginDrivenTableSink variantSink = new PluginDrivenTableSink(
                    null, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                    Collections.singletonList(payload), null, WriteOperation.MERGE, false, true, null);

            AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                    () -> variantSink.bindDataSink(Optional.empty()));
            Assert.assertTrue(exception.getMessage().contains("rolling upgrade"));
        } finally {
            Config.be_exec_version = original;
        }
    }

    @Test
    public void getExplainStringThreadsWriteOperationToHandle() {
        // WHY: EXPLAIN of a post-flip MERGE/DELETE builds a (degraded) handle for appendExplainInfo; the
        // operation is a plan-time fact available here, so it must be threaded too, otherwise a connector
        // that surfaces op-specific EXPLAIN detail (e.g. "MERGE INTO ...") shows INSERT. MUTATION: thread
        // INSERT at the getExplainString handle site -> assertion red.
        PluginDrivenExternalTable targetTable = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(targetTable.getName()).thenReturn("t1");
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider(
                new ConnectorSinkPlan(new TDataSink(TDataSinkType.ICEBERG_MERGE_SINK)));
        PluginDrivenTableSink sink = new PluginDrivenTableSink(
                targetTable, provider, null, new ConnectorTableHandle() { }, new ArrayList<>(),
                null, WriteOperation.MERGE);

        sink.getExplainString("", TExplainLevel.NORMAL);

        Assert.assertNotNull(provider.seenExplainHandle);
        Assert.assertEquals(WriteOperation.MERGE, provider.seenExplainHandle.getWriteOperation());
    }
}

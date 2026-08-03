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
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TSortInfo;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Generic data sink for plugin-driven external tables.
 *
 * <p>Extends {@link BaseExternalTableDataSink}. The connector supplies a
 * {@link ConnectorWritePlanProvider} and builds its own opaque {@link TDataSink}
 * via {@link ConnectorWritePlanProvider#planWrite}; the engine dispatches that
 * sink to BE unchanged. This is the single, source-agnostic write path used by
 * every write-capable connector (jdbc / maxcompute / iceberg). The connector-
 * specific {@code T*TableSink} dialect lives entirely inside the connector.</p>
 */
public class PluginDrivenTableSink extends BaseExternalTableDataSink {

    private final PluginDrivenExternalTable targetTable;
    // Plan-provider mode (W5): the connector builds its own opaque TDataSink via planWrite().
    private final ConnectorWritePlanProvider writePlanProvider;
    private final ConnectorSession connectorSession;
    private final ConnectorTableHandle tableHandle;
    private final List<ConnectorColumn> connectorColumns;
    private final List<ConnectorColumn> boundTargetColumns;
    // The engine-built BE sort instruction for a connector that declares write-sort columns (iceberg
    // WRITE ORDERED BY); null when the target needs no write sort. The connector cannot build it (the
    // bound output exprs live only here), so the translator resolves the connector's declared sort
    // columns against the sink output and hands the TSortInfo here to thread onto the write handle.
    private final TSortInfo writeSortInfo;
    // Opaque connector metadata generation captured before the engine shaped the physical write.
    private final String boundWriteMetadataIdentity;
    // The DML write operation this sink performs. A plain INSERT sink keeps the default INSERT (the
    // connector promotes it to OVERWRITE from the handle's isOverwrite() flag); the row-level DML
    // translator arms (DELETE / UPDATE / MERGE) pass the operation here so the connector's planWrite
    // dispatches to the matching BE sink dialect (TIcebergDeleteSink / TIcebergMergeSink) instead of
    // the INSERT TIcebergTableSink. Threaded onto the write handle so planWrite's buildWriteContext
    // reads it via ConnectorWriteHandle.getWriteOperation().
    private final WriteOperation writeOperation;
    // SQL MERGE INTO only: the statement must reject a target row matched by more than one source row.
    // Carried from PhysicalExternalRowLevelMergeSink onto the write handle so the connector can stamp the
    // enforcement flag onto its BE sink; false for UPDATE and for every non-row-level write.
    private final boolean requireMergeCardinalityCheck;

    /**
     * Plan-provider mode (W5): the connector supplies a {@link ConnectorWritePlanProvider}
     * and builds its own opaque {@link TDataSink} via
     * {@link ConnectorWritePlanProvider#planWrite}.
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns) {
        this(targetTable, writePlanProvider, connectorSession, tableHandle, connectorColumns, null);
    }

    /**
     * Plan-provider mode with an engine-built write {@link TSortInfo} threaded to the connector's write
     * handle (for a connector that declares write-sort columns).
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns,
            TSortInfo writeSortInfo) {
        this(targetTable, writePlanProvider, connectorSession, tableHandle, connectorColumns,
                writeSortInfo, WriteOperation.INSERT);
    }

    /**
     * Plan-provider mode with the DML {@link WriteOperation} threaded to the connector's write handle, so
     * the connector's {@code planWrite} dispatches to the matching BE sink dialect. The two shorter ctors
     * default this to {@link WriteOperation#INSERT} (the byte-identical plain-INSERT path); the row-level
     * DML translator arms use this ctor with {@code DELETE} / {@code UPDATE} / {@code MERGE}.
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns,
            TSortInfo writeSortInfo, WriteOperation writeOperation) {
        this(targetTable, writePlanProvider, connectorSession, tableHandle, connectorColumns,
                writeSortInfo, writeOperation, false);
    }

    /**
     * Plan-provider mode with the SQL MERGE cardinality requirement threaded to the connector's write
     * handle. Only the MERGE translator arm passes {@code true}; every other ctor defaults it to
     * {@code false}, so UPDATE / DELETE / INSERT keep their byte-identical sink.
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns,
            TSortInfo writeSortInfo, WriteOperation writeOperation, boolean requireMergeCardinalityCheck) {
        this(targetTable, writePlanProvider, connectorSession, tableHandle, connectorColumns,
                connectorColumns, writeSortInfo, writeOperation, requireMergeCardinalityCheck);
    }

    /**
     * Plan-provider mode with the write subset and complete bind-time target schema carried separately.
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns,
            List<ConnectorColumn> boundTargetColumns, TSortInfo writeSortInfo,
            WriteOperation writeOperation, boolean requireMergeCardinalityCheck) {
        this(targetTable, writePlanProvider, connectorSession, tableHandle, connectorColumns,
                boundTargetColumns, writeSortInfo, writeOperation, requireMergeCardinalityCheck, null);
    }

    /**
     * Plan-provider mode with the connector metadata generation that shaped this physical write.
     */
    public PluginDrivenTableSink(PluginDrivenExternalTable targetTable,
            ConnectorWritePlanProvider writePlanProvider, ConnectorSession connectorSession,
            ConnectorTableHandle tableHandle, List<ConnectorColumn> connectorColumns,
            List<ConnectorColumn> boundTargetColumns, TSortInfo writeSortInfo,
            WriteOperation writeOperation, boolean requireMergeCardinalityCheck,
            String boundWriteMetadataIdentity) {
        super();
        this.targetTable = targetTable;
        this.writePlanProvider = writePlanProvider;
        this.connectorSession = connectorSession;
        this.tableHandle = tableHandle;
        this.connectorColumns = connectorColumns;
        // Keep this immutable bind-time snapshot distinct from the write subset. Re-reading the live
        // table here would move the conflict-detection baseline and could miss ordinal schema drift.
        this.boundTargetColumns = ImmutableList.copyOf(boundTargetColumns);
        this.writeSortInfo = writeSortInfo;
        this.boundWriteMetadataIdentity = boundWriteMetadataIdentity;
        this.writeOperation = writeOperation == null ? WriteOperation.INSERT : writeOperation;
        this.requireMergeCardinalityCheck = requireMergeCardinalityCheck;
    }

    /**
     * The connector session this sink's write plan reads. The insert executor binds the
     * connector transaction onto it (via {@link ConnectorSession#setCurrentTransaction})
     * before {@code bindDataSink} runs, so the connector's {@code planWrite} sees the active
     * transaction.
     */
    public ConnectorSession getConnectorSession() {
        return connectorSession;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        // Connector determines format through its own write plan; accept all
        return EnumSet.allOf(TFileFormatType.class);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("PLUGIN-DRIVEN TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return sb.toString();
        }
        sb.append(prefix).append("  WRITE: plan-provider\n");
        sb.append(prefix).append("  TABLE: ").append(targetTable.getName()).append("\n");
        // Let the connector surface its own write detail (e.g. jdbc INSERT SQL); the sink itself is
        // source-agnostic. This runs before the write plan is bound (planWrite has not run yet for an
        // EXPLAIN), so the connector derives the detail from the write handle.
        ConnectorWriteHandle handle = new PluginDrivenWriteHandle(
                tableHandle, connectorColumns, boundTargetColumns, false, Collections.emptyMap(), null,
                null, Optional.empty(), writeOperation, requireMergeCardinalityCheck);
        writePlanProvider.appendExplainInfo(sb, prefix, connectorSession, handle);
        return sb.toString();
    }

    /**
     * Delegates sink construction to the connector, which returns its own opaque
     * {@link TDataSink}; the engine dispatches it to BE unchanged. The
     * {@link ConnectorWriteHandle} carries the bound target table handle and write columns.
     *
     * <p>Connector-specific write context (OVERWRITE flag, static partition spec) is read from
     * the {@link PluginDrivenInsertCommandContext} and passed through to the connector.</p>
     */
    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx)
            throws AnalysisException {
        boolean overwrite = false;
        Map<String, String> writeContext = Collections.emptyMap();
        Optional<String> branchName = Optional.empty();
        if (insertCtx.isPresent() && insertCtx.get() instanceof PluginDrivenInsertCommandContext) {
            PluginDrivenInsertCommandContext ctx = (PluginDrivenInsertCommandContext) insertCtx.get();
            overwrite = ctx.isOverwrite();
            writeContext = ctx.getStaticPartitionSpec();
            branchName = ctx.getBranchName();
        }
        ConnectorWriteHandle handle = new PluginDrivenWriteHandle(
                tableHandle, connectorColumns, boundTargetColumns, overwrite, writeContext, writeSortInfo,
                boundWriteMetadataIdentity, branchName, writeOperation, requireMergeCardinalityCheck);
        ConnectorSinkPlan sinkPlan = writePlanProvider.planWrite(connectorSession, handle);
        this.tDataSink = sinkPlan.getDataSink();
    }

    /**
     * Returns the target table.
     */
    public PluginDrivenExternalTable getTargetTable() {
        return targetTable;
    }

    /** Bound {@link ConnectorWriteHandle} passed to {@link ConnectorWritePlanProvider#planWrite}. */
    private static final class PluginDrivenWriteHandle implements ConnectorWriteHandle {
        private final ConnectorTableHandle tableHandle;
        private final List<ConnectorColumn> columns;
        private final List<ConnectorColumn> boundTargetColumns;
        private final boolean overwrite;
        private final Map<String, String> writeContext;
        private final TSortInfo sortInfo;
        private final String boundWriteMetadataIdentity;
        private final Optional<String> branchName;
        private final WriteOperation writeOperation;
        private final boolean requireMergeCardinalityCheck;

        private PluginDrivenWriteHandle(ConnectorTableHandle tableHandle, List<ConnectorColumn> columns,
                List<ConnectorColumn> boundTargetColumns, boolean overwrite,
                Map<String, String> writeContext, TSortInfo sortInfo, String boundWriteMetadataIdentity,
                Optional<String> branchName, WriteOperation writeOperation,
                boolean requireMergeCardinalityCheck) {
            this.tableHandle = tableHandle;
            this.columns = columns;
            this.boundTargetColumns = boundTargetColumns;
            this.overwrite = overwrite;
            this.writeContext = writeContext;
            this.sortInfo = sortInfo;
            this.boundWriteMetadataIdentity = boundWriteMetadataIdentity;
            this.branchName = branchName == null ? Optional.empty() : branchName;
            this.writeOperation = writeOperation == null ? WriteOperation.INSERT : writeOperation;
            this.requireMergeCardinalityCheck = requireMergeCardinalityCheck;
        }

        @Override
        public boolean isRequireMergeCardinalityCheck() {
            return requireMergeCardinalityCheck;
        }

        @Override
        public TSortInfo getSortInfo() {
            return sortInfo;
        }

        @Override
        public String getBoundWriteMetadataIdentity() {
            return boundWriteMetadataIdentity;
        }

        @Override
        public Optional<String> getBranchName() {
            return branchName;
        }

        @Override
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }

        @Override
        public List<ConnectorColumn> getColumns() {
            return columns;
        }

        @Override
        public List<ConnectorColumn> getBoundTargetColumns() {
            return boundTargetColumns;
        }

        @Override
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getStaticPartitionSpec() {
            return writeContext;
        }

        @Override
        public WriteOperation getWriteOperation() {
            return writeOperation;
        }
    }
}

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
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TSortInfo;

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
    // The engine-built BE sort instruction for a connector that declares write-sort columns (iceberg
    // WRITE ORDERED BY); null when the target needs no write sort. The connector cannot build it (the
    // bound output exprs live only here), so the translator resolves the connector's declared sort
    // columns against the sink output and hands the TSortInfo here to thread onto the write handle.
    private final TSortInfo writeSortInfo;

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
        super();
        this.targetTable = targetTable;
        this.writePlanProvider = writePlanProvider;
        this.connectorSession = connectorSession;
        this.tableHandle = tableHandle;
        this.connectorColumns = connectorColumns;
        this.writeSortInfo = writeSortInfo;
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
                tableHandle, connectorColumns, false, Collections.emptyMap(), null, Optional.empty());
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
                tableHandle, connectorColumns, overwrite, writeContext, writeSortInfo, branchName);
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
        private final boolean overwrite;
        private final Map<String, String> writeContext;
        private final TSortInfo sortInfo;
        private final Optional<String> branchName;

        private PluginDrivenWriteHandle(ConnectorTableHandle tableHandle, List<ConnectorColumn> columns,
                boolean overwrite, Map<String, String> writeContext, TSortInfo sortInfo,
                Optional<String> branchName) {
            this.tableHandle = tableHandle;
            this.columns = columns;
            this.overwrite = overwrite;
            this.writeContext = writeContext;
            this.sortInfo = sortInfo;
            this.branchName = branchName == null ? Optional.empty() : branchName;
        }

        @Override
        public TSortInfo getSortInfo() {
            return sortInfo;
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
        public boolean isOverwrite() {
            return overwrite;
        }

        @Override
        public Map<String, String> getWriteContext() {
            return writeContext;
        }
    }
}

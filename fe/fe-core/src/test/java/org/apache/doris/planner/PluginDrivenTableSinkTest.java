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

        private RecordingWritePlanProvider(ConnectorSinkPlan plan) {
            this.plan = plan;
        }

        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            this.seenSession = session;
            this.seenHandle = handle;
            return plan;
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
        Assert.assertTrue(provider.seenHandle.getWriteContext().isEmpty());
        // No engine-built write sort by default -> the handle carries no sort info.
        Assert.assertNull(provider.seenHandle.getSortInfo());
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
}

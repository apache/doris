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
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Plan-provider mode tests for {@link PluginDrivenTableSink} (W-phase W5).
 *
 * <p>When the connector supplies a {@link ConnectorWritePlanProvider}, the sink
 * must delegate {@link PluginDrivenTableSink#bindDataSink} to
 * {@link ConnectorWritePlanProvider#planWrite} and adopt the connector-built
 * opaque {@link TDataSink} verbatim, passing a {@link ConnectorWriteHandle} that
 * carries the bound target table handle and write columns. This is the seam that
 * lets connectors whose sink cannot be expressed as a generic
 * {@code ConnectorWriteConfig} (maxcompute / iceberg) produce their own
 * {@code T*TableSink}; the config-bag path is unaffected.</p>
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
    }
}

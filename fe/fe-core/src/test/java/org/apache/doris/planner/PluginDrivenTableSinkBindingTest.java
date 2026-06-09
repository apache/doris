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
import org.apache.doris.connector.ConnectorSessionBuilder;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.nereids.trees.plans.commands.insert.PluginDrivenInsertCommandContext;
import org.apache.doris.thrift.TDataSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Binding-context consumption test (P4-T06a §4.2 / gaps G4+G5).
 *
 * <p>After the cutover, INSERT OVERWRITE and INSERT ... PARTITION(col=val) against a
 * plugin-driven MaxCompute table must keep working. The commands carry the overwrite
 * flag and the static partition spec on a {@link PluginDrivenInsertCommandContext};
 * this test pins the <em>consumption</em> seam — that
 * {@link PluginDrivenTableSink#bindDataSink} forwards both into the
 * {@link ConnectorWriteHandle} handed to the connector's
 * {@link ConnectorWritePlanProvider#planWrite}. If this regresses, INSERT OVERWRITE
 * silently degrades to append and partition pinning is lost.</p>
 *
 * <p>(The production side — the command populating the context from the unbound sink —
 * is covered by post-cutover manual smoke, per the T06a test-scope decision.)</p>
 */
public class PluginDrivenTableSinkBindingTest {

    @Test
    public void overwriteAndStaticPartitionFlowToWriteHandle() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);

        PluginDrivenInsertCommandContext ctx = new PluginDrivenInsertCommandContext();
        ctx.setOverwrite(true);
        Map<String, String> staticSpec = new HashMap<>();
        staticSpec.put("pt", "20240101");
        ctx.setStaticPartitionSpec(staticSpec);

        sink.bindDataSink(Optional.of(ctx));

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertNotNull(handle, "planWrite must be invoked with a bound write handle");
        Assertions.assertTrue(handle.isOverwrite(),
                "INSERT OVERWRITE must propagate ctx.isOverwrite()=true to the connector write handle");
        Assertions.assertEquals(staticSpec, handle.getWriteContext(),
                "PARTITION(col=val) must propagate the static partition spec to the write handle");
    }

    @Test
    public void absentContextDefaultsToNonOverwriteEmptySpec() throws AnalysisException {
        RecordingWritePlanProvider provider = new RecordingWritePlanProvider();
        PluginDrivenTableSink sink = newPlanProviderSink(provider);

        sink.bindDataSink(Optional.empty());

        ConnectorWriteHandle handle = provider.capturedHandle;
        Assertions.assertNotNull(handle);
        Assertions.assertFalse(handle.isOverwrite(),
                "a plain INSERT must default the connector write handle to non-overwrite");
        Assertions.assertTrue(handle.getWriteContext().isEmpty(),
                "a plain INSERT must pass an empty static partition spec");
    }

    private static PluginDrivenTableSink newPlanProviderSink(ConnectorWritePlanProvider provider) {
        ConnectorSession session = ConnectorSessionBuilder.create().withCatalogName("mc_cat").build();
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() { };
        // targetTable is unused on the plan-provider bind path; pass null to avoid building a
        // full PluginDrivenExternalTable (which would require a catalog + database).
        return new PluginDrivenTableSink(null, provider, session, tableHandle, Collections.emptyList());
    }

    /** Records the bound {@link ConnectorWriteHandle} that the sink hands to {@code planWrite}. */
    private static final class RecordingWritePlanProvider implements ConnectorWritePlanProvider {
        private ConnectorWriteHandle capturedHandle;

        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            this.capturedHandle = handle;
            return new ConnectorSinkPlan(new TDataSink());
        }
    }
}

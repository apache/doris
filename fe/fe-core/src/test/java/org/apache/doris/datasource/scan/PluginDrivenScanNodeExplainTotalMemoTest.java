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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Guards {@link PluginDrivenScanNode#resolveUnknownTotalPartitionNum}'s memoization.
 *
 * <p>WHY: the method completes the EXPLAIN {@code partition=N/M} total for a connector-filtered
 * selection by asking the connector for its UNFILTERED view, and it runs on every render of the node
 * ({@code getNodeExplainString} is reached from {@code toString}, {@code getPlanTreeExplainStr} and the
 * query-profile path). Two shapes must be asked AT MOST ONCE per node: an UNAVAILABLE view (which leaves
 * the count unknown) and a FAILED lookup - the latter because its callers are allowed to swallow the
 * throw ({@code StmtExecutor.updateProfile} catches Throwable with a WARN), so re-querying on every
 * render would rebuild the whole view and keep aborting the profile update.</p>
 */
public class PluginDrivenScanNodeExplainTotalMemoTest {

    @Test
    public void unavailableViewIsAskedOncePerNode() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(Optional.empty());
        PluginDrivenScanNode node = node(table);

        resolve(node);
        resolve(node);

        Assertions.assertEquals(-1L, Deencapsulation.<Long>getField(node, "totalPartitionNum"),
                "an unavailable view leaves the total unknown");
        Mockito.verify(table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
    }

    @Test
    public void availableViewIsAskedOncePerNode() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(Optional.of(
                ImmutableMap.of("p1", Mockito.mock(PartitionItem.class))));
        PluginDrivenScanNode node = node(table);

        resolve(node);
        resolve(node);

        Assertions.assertEquals(1L, Deencapsulation.<Long>getField(node, "totalPartitionNum"));
        Mockito.verify(table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
    }

    @Test
    public void failedLookupIsRememberedAndRethrownWithoutReQuerying() {
        IllegalStateException failure = new IllegalStateException("metastore unavailable");
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenThrow(failure);
        PluginDrivenScanNode node = node(table);

        IllegalStateException first = Assertions.assertThrows(IllegalStateException.class, () -> resolve(node));
        IllegalStateException second = Assertions.assertThrows(IllegalStateException.class, () -> resolve(node));

        // MUTATION: memoizing only completed work -> the connector is asked again on every render, and because
        // the profile caller swallows the throw the enumeration repeats while the profile update keeps failing.
        Assertions.assertSame(failure, first);
        Assertions.assertSame(first, second, "the stored failure must be rethrown, not re-queried");
        Mockito.verify(table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
    }

    private static void resolve(PluginDrivenScanNode node) {
        Deencapsulation.invoke(node, "resolveUnknownTotalPartitionNum");
    }

    /** A scan node whose selection carries an UNKNOWN total, i.e. the shape the resolver exists for. */
    private static PluginDrivenScanNode node(PluginDrivenExternalTable table) {
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
        descriptor.setTable(table);
        PluginDrivenScanNode node = new PluginDrivenScanNode(
                new PlanNodeId(0), descriptor, false, new SessionVariable(), ScanContext.EMPTY,
                Mockito.mock(Connector.class), Mockito.mock(ConnectorSession.class),
                Mockito.mock(ConnectorTableHandle.class));
        Deencapsulation.setField(node, "totalPartitionNum", -1L);
        return node;
    }
}

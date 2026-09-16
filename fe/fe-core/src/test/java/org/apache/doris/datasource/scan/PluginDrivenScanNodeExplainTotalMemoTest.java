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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
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
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        // A FRESH exception per invocation: with a single-instance thenThrow the identity below would hold for
        // any implementation, because Mockito rethrows the same object every time.
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenAnswer(invocation -> {
            throw new IllegalStateException("metastore unavailable");
        });
        PluginDrivenScanNode node = node(table);

        IllegalStateException first = Assertions.assertThrows(IllegalStateException.class, () -> resolve(node));
        IllegalStateException second = Assertions.assertThrows(IllegalStateException.class, () -> resolve(node));

        // MUTATION: memoizing only completed work -> the connector is asked again on every render, and because
        // the profile caller swallows the throw the enumeration repeats while the profile update keeps failing.
        Assertions.assertSame(first, second, "the stored failure must be rethrown, not resolved a second time");
        Mockito.verify(table, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
    }

    @Test
    public void renderedExplainCompletesTheTotalFromOneLookup() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hive_ctl.db.tbl");
        DatabaseIf<?> db = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getDatabase()).thenReturn((DatabaseIf) db);
        Mockito.when(db.getCatalog()).thenReturn((CatalogIf) catalog);
        Mockito.when(catalog.getType()).thenReturn("hive");
        // TWO partitions in the unfiltered view, while the selection reports ONE: the asserted total must come
        // from the connector's view, not from the selection count, which a single-partition fixture could not
        // tell apart.
        Mockito.when(table.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(Optional.of(
                ImmutableMap.of("p1", Mockito.mock(PartitionItem.class),
                        "p2", Mockito.mock(PartitionItem.class))));
        PluginDrivenScanNode node = renderableNode(table);

        // MUTATION: dropping resolveUnknownTotalPartitionNum() from getNodeExplainString -> the line reads
        // partition=1/? and this fails, which the resolver-only tests above cannot see.
        Assertions.assertTrue(node.getNodeExplainString("", TExplainLevel.NORMAL).contains("partition=1/2"),
                "a connector-filtered EXPLAIN must complete the table's real total: the UNFILTERED view's size, "
                        + "not the selection's");
        // Re-render: the total is completed once per node, which the call-count assertion below pins (a repeated
        // assert on the same line would be unable to fail independently).
        node.getNodeExplainString("", TExplainLevel.NORMAL);
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

    /**
     * A node the EXPLAIN renderer can run in full without I/O: the connector session, handle and property
     * cache are pre-seeded, mirroring {@code PluginDrivenScanNodeVerboseExplainTest}'s renderable node. The
     * selection is the connector-filtered shape, i.e. a known selected count with an UNKNOWN total.
     */
    private static PluginDrivenScanNode renderableNode(PluginDrivenExternalTable table) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
        descriptor.setTable(table);
        Deencapsulation.setField(node, "desc", descriptor);
        Deencapsulation.setField(node, "conjuncts", Lists.newArrayList());
        Deencapsulation.setField(node, "scanRangeLocations", Lists.newArrayList());
        Deencapsulation.setField(node, "topnFilterSortNodes", Lists.newArrayList());
        Deencapsulation.setField(node, "scanNodeProperties", Collections.<String, String>emptyMap());
        Deencapsulation.setField(node, "isBatchModeCache", Boolean.FALSE);
        Deencapsulation.setField(node, "connector", Mockito.mock(Connector.class));
        Deencapsulation.setField(node, "pushDownAggNoGroupingOp", TPushAggOp.NONE);
        Deencapsulation.setField(node, "selectedPartitionNum", 1L);
        Deencapsulation.setField(node, "totalPartitionNum", -1L);
        return node;
    }
}

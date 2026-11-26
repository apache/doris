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
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Guards the EXPLAIN rendering of a connector-filtered selection whose table total is unknown. Rendering the
 * denominator must never initiate an unfiltered partition lookup.
 */
public class PluginDrivenScanNodeExplainUnknownTotalTest {

    @Test
    public void unknownTotalRendersAsQuestionWithoutUnfilteredLookup() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("hive_ctl.db.tbl");
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getDatabase()).thenReturn((DatabaseIf) database);
        Mockito.when(database.getCatalog()).thenReturn((CatalogIf) catalog);
        Mockito.when(catalog.getType()).thenReturn("hive");
        PluginDrivenScanNode node = renderableNode(table);

        String explain = node.getNodeExplainString("", TExplainLevel.NORMAL);

        Assertions.assertTrue(explain.contains("partition=1/?"),
                "a connector-filtered EXPLAIN must keep an unknown total unknown");
        node.getNodeExplainString("", TExplainLevel.NORMAL);
        Mockito.verify(table, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
    }

    private static PluginDrivenScanNode renderableNode(PluginDrivenExternalTable table) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
        descriptor.setTable(table);
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "desc", descriptor);
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "conjuncts", Lists.newArrayList());
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "scanRangeLocations", Lists.newArrayList());
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "topnFilterSortNodes", Lists.newArrayList());
        org.apache.doris.common.jmockit.Deencapsulation.setField(
                node, "scanNodeProperties", Collections.<String, String>emptyMap());
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "isBatchModeCache", Boolean.FALSE);
        org.apache.doris.common.jmockit.Deencapsulation.setField(
                node, "connector", Mockito.mock(Connector.class));
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "pushDownAggNoGroupingOp", TPushAggOp.NONE);
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "selectedPartitionNum", 1L);
        org.apache.doris.common.jmockit.Deencapsulation.setField(node, "totalPartitionNum", -1L);
        return node;
    }
}

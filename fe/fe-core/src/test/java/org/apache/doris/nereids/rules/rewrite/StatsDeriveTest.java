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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class StatsDeriveTest {

    @Test
    public void logicalFileScanUsesSelectedPartitionRowCount() {
        boolean previous = FeConstants.enableInternalSchemaDb;
        try {
            FeConstants.enableInternalSchemaDb = false;
            PartitionItem p1 = Mockito.mock(PartitionItem.class);
            PartitionItem p2 = Mockito.mock(PartitionItem.class);
            SelectedPartitions selectedPartitions = new SelectedPartitions(
                    2, ImmutableMap.of("p1", p1, "p2", p2), false)
                    .withPruneResult(ImmutableMap.of("p1", p1), true,
                            ImmutableList.of(), Collections.emptySet());
            ExternalTable table = Mockito.mock(ExternalTable.class);
            Mockito.when(table.initSelectedPartitions(Mockito.any())).thenReturn(selectedPartitions);
            Mockito.when(table.getRowCountForSelectedPartitions(
                    Mockito.eq(selectedPartitions), Mockito.any())).thenReturn(7L);
            SlotReference output = new SlotReference("v", IntegerType.INSTANCE);
            LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                    ImmutableList.of("db"), ImmutableList.of(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.of(ImmutableList.<Slot>of(output)));

            Statistics statistics = scan.accept(new StatsDerive(false), new StatsDerive.DeriveContext());

            Assertions.assertEquals(7, statistics.getRowCount(), 0.001);
            Assertions.assertSame(statistics, scan.getStats());
        } finally {
            FeConstants.enableInternalSchemaDb = previous;
        }
    }
}

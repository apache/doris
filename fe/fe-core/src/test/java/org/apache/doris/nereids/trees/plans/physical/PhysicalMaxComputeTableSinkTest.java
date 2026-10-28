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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.properties.DistributionSpecMaxComputeTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;

class PhysicalMaxComputeTableSinkTest {

    @Test
    void testDynamicPartitionWriteUsesShuffleThenPlainSort() {
        MaxComputeExternalTable table = mockTable(
                ImmutableList.of(new Column("part", PrimitiveType.INT)),
                ImmutableList.of(
                        new Column("data", PrimitiveType.INT),
                        new Column("part", PrimitiveType.INT)));
        List<Slot> childOutput = slots("data", "part");
        PhysicalProperties properties = PhysicalMaxComputeTableSink.buildRequirePhysicalProperties(
                table,
                ImmutableList.of(new Column("data", PrimitiveType.INT),
                        new Column("part", PrimitiveType.INT)),
                childOutput);

        Assertions.assertInstanceOf(
                DistributionSpecMaxComputeTableSinkHashPartitioned.class, properties.getDistributionSpec());
        Assertions.assertFalse(properties.getOrderSpec() instanceof MustLocalSortOrderSpec,
                "MustLocalSortOrderSpec appends a merging exchange after the partition shuffle");
        Assertions.assertInstanceOf(OrderSpec.class, properties.getOrderSpec());
        List<OrderKey> orderKeys = properties.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(1, orderKeys.size());
        Assertions.assertEquals(childOutput.get(1), orderKeys.get(0).getExpr());
    }

    @Test
    void testPartialStaticPartitionSortsByFullSchemaPositions() {
        MaxComputeExternalTable table = mockTable(
                ImmutableList.of(
                        new Column("ds", PrimitiveType.STRING),
                        new Column("region", PrimitiveType.STRING)),
                ImmutableList.of(
                        new Column("data", PrimitiveType.INT),
                        new Column("ds", PrimitiveType.STRING),
                        new Column("region", PrimitiveType.STRING)));
        List<Slot> childOutput = slots("data", "ds", "region");
        PhysicalProperties properties = PhysicalMaxComputeTableSink.buildRequirePhysicalProperties(
                table,
                ImmutableList.of(
                        new Column("data", PrimitiveType.INT),
                        new Column("region", PrimitiveType.STRING)),
                childOutput);

        Assertions.assertInstanceOf(OrderSpec.class, properties.getOrderSpec());
        List<OrderKey> orderKeys = properties.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(ImmutableList.of(childOutput.get(1), childOutput.get(2)),
                orderKeys.stream().map(OrderKey::getExpr).collect(Collectors.toList()));
    }

    private static MaxComputeExternalTable mockTable(
            List<Column> partitionColumns, List<Column> fullSchema) {
        MaxComputeExternalTable table = Mockito.mock(MaxComputeExternalTable.class);
        Mockito.when(table.getPartitionColumns()).thenReturn(partitionColumns);
        Mockito.when(table.getFullSchema()).thenReturn(fullSchema);
        return table;
    }

    private static List<Slot> slots(String... names) {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (String name : names) {
            slots.add(new SlotReference(name, IntegerType.INSTANCE));
        }
        return slots.build();
    }
}

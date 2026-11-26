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
import org.apache.doris.common.Config;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalDatabase;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecMaxComputeTableSinkHashPartitioned;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.MustLocalSortOrderSpec;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.properties.OrderSpec;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.RequestPropertyDeriver;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class PhysicalMaxComputeTableSinkTest {

    @ParameterizedTest
    @CsvSource({"12, false", "12, true", "13, false", "13, true"})
    void testDynamicPartitionWriteUsesShuffleThenPlainSort(int beExecVersion, boolean strictConsistency) {
        MaxComputeExternalTable table = mockTable(
                ImmutableList.of(new Column("part", PrimitiveType.INT)),
                ImmutableList.of(
                        new Column("data", PrimitiveType.INT),
                        new Column("part", PrimitiveType.INT)));
        List<Slot> childOutput = slots("data", "part");
        PhysicalProperties properties;
        int previousVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = beExecVersion;
            properties = deriveChildProperties(table, table.getFullSchema(), childOutput, strictConsistency);
        } finally {
            Config.be_exec_version = previousVersion;
        }

        if (beExecVersion == 12) {
            DistributionSpecHash distribution = Assertions.assertInstanceOf(
                    DistributionSpecHash.class, properties.getDistributionSpec());
            Assertions.assertEquals(ImmutableList.of(childOutput.get(1).getExprId()),
                    distribution.getOrderedShuffledColumns());
        } else {
            DistributionSpecMaxComputeTableSinkHashPartitioned distribution = Assertions.assertInstanceOf(
                    DistributionSpecMaxComputeTableSinkHashPartitioned.class, properties.getDistributionSpec());
            Assertions.assertEquals(ImmutableList.of(childOutput.get(1).getExprId()),
                    distribution.getOutputColumnExprIds());
        }
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
        PhysicalProperties properties = deriveChildProperties(
                table,
                ImmutableList.of(
                        new Column("data", PrimitiveType.INT),
                        new Column("region", PrimitiveType.STRING)),
                childOutput, true);

        Assertions.assertInstanceOf(OrderSpec.class, properties.getOrderSpec());
        List<OrderKey> orderKeys = properties.getOrderSpec().getOrderKeys();
        Assertions.assertEquals(ImmutableList.of(childOutput.get(1), childOutput.get(2)),
                orderKeys.stream().map(OrderKey::getExpr).collect(Collectors.toList()));
    }

    @Test
    void testStaticPartitionWriteHonorsStrictConsistency() {
        Column data = new Column("data", PrimitiveType.INT);
        Column part = new Column("part", PrimitiveType.INT);
        MaxComputeExternalTable table = mockTable(ImmutableList.of(part), ImmutableList.of(data, part));
        List<Slot> childOutput = slots("data", "part");

        Assertions.assertSame(PhysicalProperties.ANY,
                deriveChildProperties(table, ImmutableList.of(data), childOutput, false));
        Assertions.assertSame(PhysicalProperties.EXTERNAL_TABLE_SINK_UNPARTITIONED,
                deriveChildProperties(table, ImmutableList.of(data), childOutput, true));
    }

    private static PhysicalProperties deriveChildProperties(
            MaxComputeExternalTable table, List<Column> columns, List<Slot> childOutput, boolean strictConsistency) {
        IdGenerator<GroupId> groupIds = GroupId.createGenerator();
        LogicalProperties logical = new LogicalProperties(() -> childOutput, () -> DataTrait.EMPTY_TRAIT);
        Group child = new Group(groupIds.getNextId(), logical);
        PhysicalMaxComputeTableSink<GroupPlan> sink = new PhysicalMaxComputeTableSink<>(
                Mockito.mock(MaxComputeExternalDatabase.class), table, columns, ImmutableList.copyOf(childOutput),
                Optional.empty(), logical, new GroupPlan(child));
        GroupExpression expression = new GroupExpression(sink, ImmutableList.of(child));
        new Group(groupIds.getNextId(), expression, logical);

        SessionVariable session = new SessionVariable();
        session.setEnableStrictConsistencyDml(strictConsistency);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(session);
        RequestPropertyDeriver deriver = new RequestPropertyDeriver(context, PhysicalProperties.ANY);
        List<PhysicalProperties> childProperties = Iterables.getOnlyElement(
                deriver.getRequestChildrenPropertyList(expression));
        return Iterables.getOnlyElement(childProperties);
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

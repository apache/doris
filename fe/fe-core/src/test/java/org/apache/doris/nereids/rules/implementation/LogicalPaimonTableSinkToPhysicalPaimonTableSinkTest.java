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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPaimonTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPaimonTableSink;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogicalPaimonTableSinkToPhysicalPaimonTableSinkTest {

    @Test
    public void testLogicalPaimonSinkKeepsChildUnchanged() {
        SlotReference partitionSlot = new SlotReference("pt", IntegerType.INSTANCE);
        SlotReference bucketSlot = new SlotReference("bucket_key", IntegerType.INSTANCE);
        SlotReference valueSlot = new SlotReference("v", StringType.INSTANCE);
        List<SlotReference> outputSlots = Arrays.asList(partitionSlot, bucketSlot, valueSlot);

        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Plan child = Mockito.mock(Plan.class);
        Mockito.when(child.getAllChildrenTypes()).thenReturn(new BitSet());
        Mockito.when(child.depth()).thenReturn(1);
        Mockito.when(child.getOutput()).thenReturn(outputSlots.stream()
                .map(Slot.class::cast)
                .collect(Collectors.toList()));
        List<Slot> output = outputSlots.stream()
                .map(Slot.class::cast)
                .collect(Collectors.toList());
        List<NamedExpression> outputExprs = outputSlots.stream()
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        LogicalPaimonTableSink<Plan> sink = new LogicalPaimonTableSink<>(
                Mockito.mock(PaimonExternalDatabase.class),
                table,
                Arrays.asList(
                        new Column("pt", PrimitiveType.INT),
                        new Column("bucket_key", PrimitiveType.INT),
                        new Column("v", PrimitiveType.STRING)),
                outputExprs,
                DMLCommandType.INSERT,
                Optional.empty(),
                Optional.of(new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT)),
                child);

        Rule rule = new LogicalPaimonTableSinkToPhysicalPaimonTableSink().build();
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(Mockito.mock(StatementContext.class));
        Plan transformed = rule.transform(sink, cascadesContext).get(0);

        Assert.assertTrue(transformed instanceof PhysicalPaimonTableSink);
        Assert.assertSame(child, ((PhysicalPaimonTableSink<?>) transformed).child());
    }
}

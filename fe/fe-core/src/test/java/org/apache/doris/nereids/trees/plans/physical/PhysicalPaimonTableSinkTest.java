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

import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DistributionSpecPaimonBucketShuffle;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PhysicalPaimonTableSinkTest {

    @Test
    public void testRequirePhysicalPropertiesUsesPartitionShuffle() throws Exception {
        SlotReference ptSlot = new SlotReference("pt", IntegerType.INSTANCE);
        List<NamedExpression> projects = Collections.singletonList(ptSlot);
        List<Slot> output = Collections.singletonList(ptSlot);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getPartitionColumnNames(Mockito.any())).thenReturn(ImmutableSet.of("pt"));
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("tbl1");

        LogicalProperties logicalProperties = new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT);
        PhysicalEmptyRelation child = new PhysicalEmptyRelation(new RelationId(1), projects, logicalProperties);

        PhysicalPaimonTableSink<PhysicalEmptyRelation> sink = new PhysicalPaimonTableSink<>(database, table,
                Collections.emptyList(), Collections.emptyList(), Optional.empty(), logicalProperties, child);

        PhysicalProperties required = sink.getRequirePhysicalProperties();
        Assertions.assertTrue(required.getDistributionSpec() instanceof DistributionSpecPaimonBucketShuffle);
    }

    @Test
    public void testRequirePhysicalPropertiesFallsBackWhenPartitionSlotsMissing() throws Exception {
        SlotReference k1Slot = new SlotReference("k1", IntegerType.INSTANCE);
        List<NamedExpression> projects = Collections.singletonList(k1Slot);
        List<Slot> output = Collections.singletonList(k1Slot);
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getPartitionColumnNames(Mockito.any())).thenReturn(ImmutableSet.of("pt"));
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("tbl1");

        LogicalProperties logicalProperties = new LogicalProperties(() -> output, () -> DataTrait.EMPTY_TRAIT);
        PhysicalEmptyRelation child = new PhysicalEmptyRelation(new RelationId(2), projects, logicalProperties);

        PhysicalPaimonTableSink<PhysicalEmptyRelation> sink = new PhysicalPaimonTableSink<>(database, table,
                Collections.emptyList(), Collections.emptyList(), Optional.empty(), logicalProperties, child);

        Assertions.assertSame(PhysicalProperties.SINK_RANDOM_PARTITIONED, sink.getRequirePhysicalProperties());
    }
}

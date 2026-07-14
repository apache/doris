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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;

public class LazyMaterializeTopNTest {

    @Test
    public void testEffectiveOutputRemovesDuplicateSlotsWithoutChangingOrder() {
        SlotReference first = new SlotReference("first", IntegerType.INSTANCE);
        SlotReference second = new SlotReference("second", IntegerType.INSTANCE);

        Assertions.assertEquals(ImmutableList.of(first, second),
                LazyMaterializeTopN.distinctOutput(ImmutableList.of(first, second, first)));
    }

    @Test
    public void testScanRowIdRemainsNonNullableForNullableMaterializationInput() {
        SlotReference nullableRowId = (SlotReference) new SlotReference("row_id", StringType.INSTANCE)
                .withNullable(true);
        Relation relation = Mockito.mock(Relation.class);
        Mockito.when(relation.getRelationId()).thenReturn(new RelationId(1));
        Column baseColumn = new Column("base", Type.INT);
        baseColumn.setUniqueId(1);
        SlotReference baseSlot = new SlotReference("base", IntegerType.INSTANCE).withColumn(baseColumn);
        LazySourceSpec source = new LazySourceSpec(
                relation, nullableRowId, ImmutableList.of(new DeferredColumnSpec(
                        SourceColumnKey.from(relation, baseSlot), baseSlot, 0, ImmutableList.of(baseSlot))));

        Assertions.assertTrue(source.getRowIdSlot().nullable());
        Assertions.assertFalse(source.getScanRowIdSlot().nullable());
        Assertions.assertEquals(source.getRowIdSlot().getExprId(), source.getScanRowIdSlot().getExprId());
    }

    @Test
    public void testAliasIsRequiredWhenItsBaseSlotIsMaterialized() {
        Column baseColumn = new Column("base", Type.INT);
        baseColumn.setUniqueId(1);
        SlotReference baseSlot = new SlotReference("base", IntegerType.INSTANCE).withColumn(baseColumn);
        Slot aliasSlot = new Alias(baseSlot, "alias").toSlot();
        Column independentColumn = new Column("independent", Type.INT);
        independentColumn.setUniqueId(2);
        SlotReference independentBaseSlot = new SlotReference("independent", IntegerType.INSTANCE)
                .withColumn(independentColumn);
        Slot independentAliasSlot = new Alias(independentBaseSlot, "independent_alias").toSlot();
        Relation relation = Mockito.mock(Relation.class);
        Mockito.when(relation.getRelationId()).thenReturn(new RelationId(1));
        Map<Slot, MaterializeSource> materializeMap = new LinkedHashMap<>();
        materializeMap.put(aliasSlot, new MaterializeSource(relation, baseSlot));
        materializeMap.put(independentAliasSlot, new MaterializeSource(relation, independentBaseSlot));

        TopNLazyMaterializationAnalyzer.removeConflictingCandidates(
                materializeMap, ImmutableSet.of(), ImmutableSet.of(baseSlot));

        Assertions.assertEquals(ImmutableList.of(independentAliasSlot), ImmutableList.copyOf(materializeMap.keySet()));
    }
}

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

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TopNLazyMaterializationSpecTest {

    @Test
    public void testSourceColumnKeyNormalizesAccessPathOrder() {
        PhysicalOlapScan relation = relation(2);
        SlotReference left = slot("value", 7).withAccessPaths(ImmutableList.of(
                ColumnAccessPath.data(ImmutableList.of("b")),
                ColumnAccessPath.data(ImmutableList.of("a"))), ImmutableList.of());
        SlotReference right = slot("value", 7).withAccessPaths(ImmutableList.of(
                ColumnAccessPath.data(ImmutableList.of("a")),
                ColumnAccessPath.data(ImmutableList.of("b"))), ImmutableList.of());

        Assertions.assertEquals(SourceColumnKey.from(relation, left), SourceColumnKey.from(relation, right));
        Assertions.assertEquals(SourceColumnKey.from(relation, left).hashCode(),
                SourceColumnKey.from(relation, right).hashCode());
    }

    @Test
    public void testDeferredColumnUsesOriginalNameForBaseIndex() {
        PhysicalOlapScan relation = relation(1);
        Mockito.when(relation.getTable().getBaseColumnIdxByName("value")).thenReturn(3);
        SlotReference baseSlot = slot("value", 7);
        Slot alias = new Alias(baseSlot, "renamed").toSlot();

        DeferredColumnSpec deferred = DeferredColumnSpec.from(relation, baseSlot, ImmutableList.of(alias));

        Assertions.assertEquals(3, deferred.getBaseColumnIndex());
        Mockito.verify(relation.getTable()).getBaseColumnIdxByName("value");
        Mockito.verify(relation.getTable(), Mockito.never()).getBaseColumnIdxByName("renamed");
    }

    @Test
    public void testSpecUsesImmutableOrderedSources() {
        PhysicalOlapScan secondRelation = relation(2);
        PhysicalOlapScan firstRelation = relation(1);
        SlotReference materialized = slot("key", 1);
        SlotReference firstBase = slot("first", 2);
        SlotReference secondBase = slot("second", 3);
        Slot firstOutput = new Alias(firstBase, "first_alias").toSlot();
        Slot secondOutput = new Alias(secondBase, "second_alias").toSlot();
        SlotReference firstRowId = slot("first_row_id", 10);
        SlotReference secondRowId = slot("second_row_id", 11);
        LazySourceSpec secondSource = new LazySourceSpec(secondRelation, secondRowId,
                ImmutableList.of(new DeferredColumnSpec(SourceColumnKey.from(secondRelation, secondBase),
                        secondBase, 1, ImmutableList.of(secondOutput))));
        LazySourceSpec firstSource = new LazySourceSpec(firstRelation, firstRowId,
                ImmutableList.of(new DeferredColumnSpec(SourceColumnKey.from(firstRelation, firstBase),
                        firstBase, 1, ImmutableList.of(firstOutput))));
        List<LazySourceSpec> mutableSources = new ArrayList<>(ImmutableList.of(secondSource, firstSource));

        TopNLazyMaterializationSpec spec = new TopNLazyMaterializationSpec(
                ImmutableList.of(materialized, firstOutput, secondOutput),
                ImmutableList.of(materialized),
                ImmutableList.of(materialized, firstRowId, secondRowId), mutableSources);
        mutableSources.clear();

        Assertions.assertEquals(ImmutableList.of(firstSource, secondSource), spec.getSources());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> spec.getSources().clear());

    }

    @Test
    public void testDuplicateSourceColumnIsRejected() {
        PhysicalOlapScan relation = relation(1);
        SlotReference base = slot("value", 7);
        SlotReference rowId = slot("row_id", 10);
        DeferredColumnSpec first = new DeferredColumnSpec(SourceColumnKey.from(relation, base),
                base, 1, ImmutableList.of(base));
        DeferredColumnSpec duplicate = new DeferredColumnSpec(SourceColumnKey.from(relation, base),
                base, 1, ImmutableList.of(new Alias(base, "alias").toSlot()));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LazySourceSpec(relation, rowId, ImmutableList.of(first, duplicate)));
    }

    @Test
    public void testMisalignedRowIdOrderIsRejected() {
        PhysicalOlapScan relation = relation(1);
        SlotReference base = slot("value", 7);
        SlotReference rowId = slot("row_id", 10);
        SlotReference wrongRowId = slot("wrong_row_id", 11);
        LazySourceSpec source = new LazySourceSpec(relation, rowId,
                ImmutableList.of(new DeferredColumnSpec(SourceColumnKey.from(relation, base),
                        base, 1, ImmutableList.of(base))));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new TopNLazyMaterializationSpec(ImmutableList.of(base), ImmutableList.of(),
                        ImmutableList.of(wrongRowId), ImmutableList.of(source)));
    }

    private PhysicalOlapScan relation(int relationId) {
        PhysicalOlapScan relation = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(relation.getRelationId()).thenReturn(new RelationId(relationId));
        Mockito.when(relation.getTable()).thenReturn(Mockito.mock(OlapTable.class));
        return relation;
    }

    private SlotReference slot(String name, int uniqueId) {
        Column column = new Column(name, Type.INT);
        column.setUniqueId(uniqueId);
        return new SlotReference(name, IntegerType.INSTANCE).withColumn(column);
    }
}

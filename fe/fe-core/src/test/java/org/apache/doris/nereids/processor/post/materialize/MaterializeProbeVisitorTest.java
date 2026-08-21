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

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MaterializeProbeVisitorTest {

    @Test
    void testVectorSearchSupportsLazyMaterialization() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();

        Assertions.assertTrue(visitor.checkTVFRelationTableSupportedType(relation));
    }

    @Test
    void testVectorSearchKeepsNestedSubColumnInSearchPhase() {
        MaterializeProbeVisitor visitor = new MaterializeProbeVisitor();
        PhysicalTVFRelation relation = mockVectorSearchRelation();
        SlotReference nestedSlot = Mockito.mock(SlotReference.class);
        Mockito.when(nestedSlot.hasSubColPath()).thenReturn(true);

        Assertions.assertFalse(visitor.visitPhysicalTVFRelation(
                relation, new MaterializeProbeVisitor.ProbeContext(nestedSlot)).isPresent());
    }

    private PhysicalTVFRelation mockVectorSearchRelation() {
        PhysicalTVFRelation relation = Mockito.mock(PhysicalTVFRelation.class);
        VectorSearch function = Mockito.mock(VectorSearch.class);
        Mockito.when(function.getName()).thenReturn(VectorSearchTableValuedFunction.NAME);
        Mockito.when(relation.getFunction()).thenReturn(function);
        return relation;
    }
}

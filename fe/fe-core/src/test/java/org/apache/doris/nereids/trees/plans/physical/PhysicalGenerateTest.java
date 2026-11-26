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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.processor.post.Validator;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PhysicalGenerateTest {
    @Test
    void testLateralConjunctExpressionsAndSlots() {
        Alias array = new Alias(new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1))), "xs");
        Alias threshold = new Alias(new IntegerLiteral(0), "threshold");
        LogicalProperties childProperties = new LogicalProperties(
                () -> ImmutableList.of(array.toSlot(), threshold.toSlot()), () -> DataTrait.EMPTY_TRAIT);
        PhysicalOneRowRelation child = new PhysicalOneRowRelation(
                new RelationId(1), ImmutableList.of(array, threshold), childProperties);
        Unnest generator = new Unnest(array.toSlot());
        Slot x = new SlotReference("x", IntegerType.INSTANCE);
        AssertTrue conjunct = new AssertTrue(new GreaterThan(x, threshold.toSlot()), new StringLiteral("bad"));
        PhysicalGenerate<?> generate = new PhysicalGenerate<>(ImmutableList.of(generator),
                ImmutableList.of(x), ImmutableList.of(conjunct), null, child);

        Assertions.assertEquals(ImmutableList.of(generator, conjunct), generate.getExpressions());
        Assertions.assertEquals(ImmutableSet.of(array.toSlot(), threshold.toSlot()), generate.getInputSlots());
        Assertions.assertDoesNotThrow(() -> Validator.checkAllSlotFromChildren(generate));

        Slot missing = new SlotReference("missing", IntegerType.INSTANCE);
        PhysicalGenerate<?> invalid = new PhysicalGenerate<>(generate.getGenerators(), generate.getGeneratorOutput(),
                ImmutableList.of(new GreaterThan(x, missing)), null, child);
        Assertions.assertThrows(AnalysisException.class, () -> Validator.checkAllSlotFromChildren(invalid));
    }
}

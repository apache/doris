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

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalJdbcScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanConstructor;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for {@link AdjustNullableTest}.
 */
class AdjustNullableTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testLogicalExternalRelation() {
        new MockUp<LogicalJdbcScan>() {
            @Mock
            public Set<Slot> getOutputSet() {
                Set<Slot> output = new HashSet<>();
                output.add(new SlotReference(new ExprId(1), "id", IntegerType.INSTANCE, false,
                        new ArrayList<>()));
                return output;
            }
        };

        GreaterThan gt = new GreaterThan(new SlotReference(new ExprId(1), "id",
                IntegerType.INSTANCE, true, new ArrayList<>()), Literal.of("1"));
        Set<Expression> conjuncts = new HashSet<>();
        conjuncts.add(gt);
        Assertions.assertTrue(conjuncts.iterator().next().nullable());
        LogicalJdbcScan jdbcScan =
                new LogicalJdbcScan(new RelationId(1), PlanConstructor.newOlapTable(0, "t1", 0),
                        new ArrayList<>(), Optional.empty(), Optional.empty(), conjuncts);
        AdjustNullable adjustNullable = new AdjustNullable();
        LogicalJdbcScan newJdbcScan = (LogicalJdbcScan) adjustNullable.rewriteRoot(jdbcScan, null);
        conjuncts = newJdbcScan.getConjuncts();
        Assertions.assertFalse(conjuncts.iterator().next().nullable());
    }
}

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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

class PushProjectIntoUnionTest {
    @Test
    void testFoldedDecimalRowsMatchUnionOutput() {
        DecimalV3Type type = DecimalV3Type.createDecimalV3Type(5, 4);
        SlotReference input = new SlotReference("amount", type, false);
        Alias first = new Alias(new DecimalV3Literal(type, new BigDecimal("-0.2045")), "amount");
        Alias second = new Alias(new DecimalV3Literal(type, new BigDecimal("-1.0101")), "amount");
        LogicalUnion union = new LogicalUnion(Qualifier.ALL, ImmutableList.of(input), ImmutableList.of(),
                ImmutableList.of(ImmutableList.of(first), ImmutableList.of(second)), false, ImmutableList.of());
        LogicalProject<LogicalUnion> project = new LogicalProject<>(
                ImmutableList.of(new Alias(new Abs(input), "magnitude")), union);

        LogicalUnion rewritten = (LogicalUnion) PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushProjectIntoUnion())
                .getPlan();
        Assertions.assertEquals(type, rewritten.getOutput().get(0).getDataType());
        Assertions.assertEquals(2, rewritten.getConstantExprsList().size());
        for (List<NamedExpression> row : rewritten.getConstantExprsList()) {
            Assertions.assertTrue(row.get(0).child(0) instanceof DecimalV3Literal);
            Assertions.assertEquals(rewritten.getOutput().get(0).getDataType(), row.get(0).getDataType());
        }
    }
}

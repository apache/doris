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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExpressionShapeInfoTest {

    @Test
    void testWrappedAggregateShapeInfoPreservesQualifier() {
        assertPreservesQualifier(new Cast(new Abs(avg()), DoubleType.INSTANCE));
        assertPreservesQualifier(new TryCast(avg(), DoubleType.INSTANCE));
        assertPreservesQualifier(new BitNot(avg()));
        assertPreservesQualifier(new Alias(new Cast(avg(), DoubleType.INSTANCE), "avg_alias"));
        assertPreservesQualifier(new AggregateExpression(avg(), AggregateParam.LOCAL_RESULT));
        assertPreservesQualifier(new Not(new IsNull(avg())));
        assertPreservesQualifier(new Between(avg(), new IntegerLiteral(1), new IntegerLiteral(2)));
        assertPreservesQualifier(new InPredicate(avg(), ImmutableList.of(new IntegerLiteral(1))));
        assertPreservesQualifier(new CaseWhen(
                ImmutableList.of(new WhenClause(new IsNull(avg()), new Cast(avg(), DoubleType.INSTANCE))),
                new IntegerLiteral(0)));
    }

    private void assertPreservesQualifier(Expression expression) {
        String shapeInfo = expression.shapeInfo();
        Assertions.assertTrue(shapeInfo.contains("avg(t1.c11)"), shapeInfo);
        Assertions.assertFalse(shapeInfo.contains("avg(c11)"), shapeInfo);
    }

    private Avg avg() {
        return new Avg(new SlotReference("c11", IntegerType.INSTANCE, true, ImmutableList.of("t1")));
    }
}

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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PredicateRewriteForPartitionFilterTest extends ExpressionRewriteTestHelper {

    @Test
    public void testVisitInPredicate() {
        SlotReference test = new SlotReference("test", IntegerType.INSTANCE);
        IntegerLiteral i1 = new IntegerLiteral(1);
        IntegerLiteral i2 = new IntegerLiteral(2);
        IntegerLiteral i3 = new IntegerLiteral(3);
        InPredicate inPredicate = new InPredicate(test, Lists.newArrayList(i1, i2, i3));

        Expression child = inPredicate.getCompareExpr();
        List<Expression> splitIn = new ArrayList<>();
        for (Expression opt : inPredicate.getOptions()) {
            splitIn.add(new EqualTo(child, opt));
        }
        Expression or = ExpressionUtils.compound(false, splitIn);
        Assertions.assertEquals(or, PredicateRewriteForPartitionFilter.rewrite(inPredicate, context.cascadesContext));
    }
}

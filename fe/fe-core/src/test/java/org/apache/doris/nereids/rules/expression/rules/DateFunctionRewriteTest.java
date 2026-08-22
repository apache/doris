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
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DateFunctionRewriteTest extends ExpressionRewriteTestHelper {
    private Expression timestampNs;

    @BeforeEach
    void setUp() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(DateFunctionRewrite.INSTANCE)
        ));
        timestampNs = new SlotReference("timestampNs", TimeStampNsType.INSTANCE, true);
    }

    @Test
    void testRewriteTimeStampNsDateComparisons() {
        Date dateFunction = new Date(timestampNs);
        DateV2Literal date = new DateV2Literal("2024-01-02");
        TimeStampNsLiteral start = new TimeStampNsLiteral("2024-01-02 00:00:00.000000000");
        TimeStampNsLiteral end = new TimeStampNsLiteral("2024-01-02 23:59:59.999999999");

        assertRewrite(new EqualTo(dateFunction, date), new And(
                new GreaterThanEqual(timestampNs, start), new LessThanEqual(timestampNs, end)));
        assertRewrite(new GreaterThan(dateFunction, date), new GreaterThan(timestampNs, end));
        assertRewrite(new GreaterThanEqual(dateFunction, date), new GreaterThanEqual(timestampNs, start));
        assertRewrite(new LessThan(dateFunction, date), new LessThan(timestampNs, start));
        assertRewrite(new LessThanEqual(dateFunction, date), new LessThanEqual(timestampNs, end));
    }

    @Test
    void testRewriteTimeStampNsDateComparisonsAtTypeBounds() {
        Date dateFunction = new Date(timestampNs);
        DateV2Literal minDate = new DateV2Literal("1677-09-21");
        DateV2Literal maxDate = new DateV2Literal("2262-04-11");

        assertRewrite(new GreaterThanEqual(dateFunction, minDate),
                new GreaterThanEqual(timestampNs, TimeStampNsLiteral.getMinValue()));
        assertRewrite(new LessThanEqual(dateFunction, maxDate),
                new LessThanEqual(timestampNs, TimeStampNsLiteral.getMaxValue()));

        DateV2Literal beforeMin = new DateV2Literal("1677-09-20");
        DateV2Literal afterMax = new DateV2Literal("2262-04-12");
        assertRewrite(new LessThan(dateFunction, beforeMin), ExpressionUtils.falseOrNull(timestampNs));
        assertRewrite(new GreaterThan(dateFunction, afterMax), ExpressionUtils.falseOrNull(timestampNs));
        assertRewrite(new GreaterThanEqual(dateFunction, beforeMin), ExpressionUtils.trueOrNull(timestampNs));
        assertRewrite(new LessThanEqual(dateFunction, afterMax), ExpressionUtils.trueOrNull(timestampNs));
    }
}

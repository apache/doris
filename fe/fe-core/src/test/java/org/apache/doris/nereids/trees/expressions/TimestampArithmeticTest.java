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

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Interval;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimestampArithmeticTest {

    @Test
    public void testToSql() {
        TimestampArithmetic timestampArithmetic = new TimestampArithmetic("days_sub",
                ArithmeticExpr.Operator.ADD, new Alias(new DateLiteral("2025-09-30"), "CURRENT_DATE"),
                new IntegerLiteral(3), Interval.TimeUnit.DAY);
        Assertions.assertEquals("days_sub('2025-09-30', INTERVAL 3 DAY)", timestampArithmetic.toSql());
        timestampArithmetic = new TimestampArithmetic(null,
                ArithmeticExpr.Operator.ADD, new Alias(new DateLiteral("2025-09-30"), "CURRENT_DATE"),
                new IntegerLiteral(3), Interval.TimeUnit.DAY);
        Assertions.assertEquals("'2025-09-30' + INTERVAL 3 DAY", timestampArithmetic.toSql());
    }
}

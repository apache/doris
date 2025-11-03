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

import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionTest {
    @Test
    public void testConstantExpression() {
        // literal is constant
        Assertions.assertTrue(new StringLiteral("abc").isConstant());

        // slot reference is not constant
        Assertions.assertFalse(new SlotReference("a", IntegerType.INSTANCE).isConstant());

        // `1 + 2` is constant
        Assertions.assertTrue(new Add(new IntegerLiteral(1), new IntegerLiteral(2)).isConstant());

        // `a + 1` is not constant
        Assertions.assertFalse(
                new Add(new SlotReference("a", IntegerType.INSTANCE), new IntegerLiteral(1)).isConstant());
    }

    @Test
    public void testContainsNondeterministic() {
        Assertions.assertTrue(new DaysAdd(new CurrentDate(), new IntegerLiteral(2)).containsNondeterministic());
        Assertions.assertTrue(new DaysAdd(
                new UnixTimestamp(
                        new CurrentDate(),
                        new VarcharLiteral("%Y-%m-%d %H:%i-%s")), new IntegerLiteral(2))
                .containsNondeterministic());
        Assertions.assertTrue(new DaysAdd(
                new UnixTimestamp(new CurrentDate()), new IntegerLiteral(2)).containsNondeterministic());

        Assertions.assertFalse(new DaysAdd(
                new UnixTimestamp(
                        new DateLiteral("2024-01-01"),
                        new VarcharLiteral("%Y-%m-%d %H:%i-%s")), new IntegerLiteral(2))
                .containsNondeterministic());
        Assertions.assertTrue(new DaysAdd(
                new UnixTimestamp(), new IntegerLiteral(2)).containsNondeterministic());

    }
}

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

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class InferPredicateByReplaceTest {
    @Test
    public void testInferWithEqualTo() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(1, result.size(), "Expected no additional predicates.");
    }

    @Test
    public void testInferWithInPredicate() {
        // abs(a) IN (1, 2, 3)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(new Abs(a),
                ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(inPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testInferWithInPredicateNotSupport() {
        // a IN (1, b)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(a,
                ImmutableList.of(new IntegerLiteral(1), b));
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(inPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testInferWithNotPredicate() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        InPredicate inPredicate = new InPredicate(a, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)));
        Not notPredicate = new Not(inPredicate);
        EqualTo equalTo = new EqualTo(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(notPredicate);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Not expected = new Not(new InPredicate(b, ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))));
        Assertions.assertTrue(result.contains(expected));
    }

    @Test
    public void testInferWithLikePredicate() {
        // a LIKE 'test%'
        SlotReference a = new SlotReference("a", StringType.INSTANCE);
        SlotReference b = new SlotReference("b", StringType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Like like = new Like(a, new StringLiteral("test%"));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(like);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Like expected = new Like(b, new StringLiteral("test%"));
        Assertions.assertEquals(3, result.size());
        Assertions.assertTrue(result.contains(expected), "Expected to find b like 'test%' in the result");
    }

    @Test
    public void testInferWithLikePredicateNotSupport() {
        // a LIKE b
        SlotReference a = new SlotReference("a", StringType.INSTANCE);
        SlotReference b = new SlotReference("b", StringType.INSTANCE);
        EqualTo equalTo = new EqualTo(a, b);
        Like like = new Like(a, b);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(like);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testInferWithOrPredicate() {
        SlotReference a = new SlotReference("a", DateTimeV2Type.SYSTEM_DEFAULT);
        SlotReference b = new SlotReference("b", DateTimeV2Type.SYSTEM_DEFAULT);
        EqualTo equalTo = new EqualTo(a, b);
        Or or = new Or(new GreaterThan(a, new DateTimeV2Literal("2022-02-01 10:00:00")),
                new LessThan(a, new DateTimeV2Literal("2022-01-01 10:00:00")));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(or);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testInferWithPredicateDateTrunc() {
        SlotReference a = new SlotReference("a", DateTimeV2Type.SYSTEM_DEFAULT);
        SlotReference b = new SlotReference("b", DateTimeV2Type.SYSTEM_DEFAULT);
        EqualTo equalTo = new EqualTo(a, b);
        GreaterThan greaterThan = new GreaterThan(new DateTrunc(a, new VarcharLiteral("year")), new DateTimeV2Literal("2022-02-01 10:00:00"));
        Set<Expression> inputs = new HashSet<>();
        inputs.add(greaterThan);
        inputs.add(equalTo);

        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(3, result.size());
    }

    @Test
    public void testValidForInfer() {
        SlotReference a = new SlotReference("a", TinyIntType.INSTANCE);
        Cast castExprA = new Cast(a, IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", BigIntType.INSTANCE);
        Cast castExprB = new Cast(b, IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", DateType.INSTANCE);
        Cast castExprC = new Cast(c, IntegerType.INSTANCE);

        EqualTo equalTo1 = new EqualTo(castExprA, castExprB);
        EqualTo equalTo2 = new EqualTo(castExprA, castExprC);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Assertions.assertEquals(2, InferPredicateByReplace.infer(inputs).size());
    }

    @Test
    public void testNotInferWithTransitiveEqualitySameTable() {
        // a = b, b = c
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE, true, ImmutableList.of("t1"));
        EqualTo equalTo1 = new EqualTo(a, b);
        EqualTo equalTo2 = new EqualTo(b, c);
        Set<Expression> inputs = new HashSet<>();
        inputs.add(equalTo1);
        inputs.add(equalTo2);
        Set<Expression> result = InferPredicateByReplace.infer(inputs);
        Assertions.assertEquals(2, result.size());
    }
}

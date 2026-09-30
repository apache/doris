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

package org.apache.doris.nereids.util;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayShuffle;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Uuid;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.PythonUdf;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CacheExpressionTest {
    @Test
    void testNestedExpressionsAndConjunctOrder() {
        Expression stable = new Add(new SlotReference("k", IntegerType.INSTANCE), new IntegerLiteral(1));
        Assertions.assertFalse(uncacheable(stable));
        for (Expression unstable : ImmutableList.of(new Random(), new Random(new BigIntLiteral(1)),
                new Uuid(), new Now())) {
            Assertions.assertTrue(uncacheable(new Cast(unstable, StringType.INSTANCE)));
            Assertions.assertTrue(ExpressionUtils.containsNonCacheableExpression(ImmutableList.of(stable, unstable)));
            Assertions.assertTrue(ExpressionUtils.containsNonCacheableExpression(ImmutableList.of(unstable, stable)));
        }
    }

    @Test
    void testAllUdfVolatilitiesRemainExcluded() {
        Expression slot = new SlotReference("k", IntegerType.INSTANCE);
        FunctionSignature signature = FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE);
        for (FunctionVolatility volatility : FunctionVolatility.values()) {
            VolatileIdentity identity = Udf.createVolatileIdentity(volatility);
            Expression javaUdf = new JavaUdf("f", 1, "db", Function.BinaryType.JAVA_UDF, signature,
                    Function.NullableMode.DEPEND_ON_ARGUMENT, volatility, identity,
                    "file:///udf.jar", "F", "", "", "", false, 360, slot);
            Expression pythonUdf = new PythonUdf("f", 2, "db", Function.BinaryType.PYTHON_UDF, signature,
                    Function.NullableMode.DEPEND_ON_ARGUMENT, volatility, identity,
                    "", "f", "", "", "", false, 360, "3.12", "", slot);
            Assertions.assertEquals(volatility == FunctionVolatility.IMMUTABLE, javaUdf.isDeterministic());
            Assertions.assertTrue(uncacheable(javaUdf));
            Assertions.assertTrue(uncacheable(pythonUdf));
        }
    }

    @Test
    void testShuffleIsVolatileWithAndWithoutSeed() {
        Expression array = new SlotReference("a", ArrayType.of(IntegerType.INSTANCE));
        for (ArrayShuffle shuffle : ImmutableList.of(new ArrayShuffle(array),
                new ArrayShuffle(array, new BigIntLiteral(1)))) {
            Assertions.assertTrue(uncacheable(shuffle));
            Assertions.assertFalse(shuffle.foldable());
            Assertions.assertEquals(shuffle, shuffle.withChildren(shuffle.children()));
        }
        ArrayShuffle first = new ArrayShuffle(array, new BigIntLiteral(1));
        ArrayShuffle second = new ArrayShuffle(array, new BigIntLiteral(1));
        Assertions.assertNotEquals(first, second);
        Assertions.assertEquals(first.withIgnoreUniqueId(true), second.withIgnoreUniqueId(true));
    }

    @Test
    void testTimeCastsDependOnQueryDate() {
        Expression time = new SlotReference("t", TimeV2Type.SYSTEM_DEFAULT);
        for (DataType target : ImmutableList.of(DateType.INSTANCE, DateV2Type.INSTANCE,
                DateTimeType.INSTANCE, DateTimeV2Type.SYSTEM_DEFAULT, DateTimeV2Type.of(6),
                TimeStampTzType.SYSTEM_DEFAULT)) {
            Cast cast = new Cast(time, target);
            Assertions.assertFalse(cast.isDeterministic());
            Assertions.assertFalse(cast.foldable());
            Assertions.assertTrue(uncacheable(new Cast(cast, StringType.INSTANCE)));
        }
        Assertions.assertFalse(uncacheable(new Cast(time, StringType.INSTANCE)));
        Assertions.assertFalse(uncacheable(new Cast(time, TimeV2Type.of(6))));
        Assertions.assertFalse(uncacheable(new Cast(
                new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT), DateV2Type.INSTANCE)));
    }

    private boolean uncacheable(Expression expression) {
        return ExpressionUtils.containsNonCacheableExpression(ImmutableList.of(expression));
    }
}

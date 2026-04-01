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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DoubleType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RandSparkTest {

    @Test
    public void testRandSparkNoArgs() {
        RandSpark randSpark = new RandSpark();
        Assertions.assertEquals("rand_spark", randSpark.getName());
        Assertions.assertEquals(0, randSpark.arity());
        Assertions.assertEquals(DoubleType.INSTANCE, randSpark.getDataType());
        Assertions.assertFalse(randSpark.nullable());
    }

    @Test
    public void testRandSparkWithSeed() {
        RandSpark randSpark = new RandSpark(new BigIntLiteral(42L));
        Assertions.assertEquals("rand_spark", randSpark.getName());
        Assertions.assertEquals(1, randSpark.arity());
        Assertions.assertEquals(DoubleType.INSTANCE, randSpark.getDataType());
        Assertions.assertFalse(randSpark.nullable());
    }

    @Test
    public void testRandSparkWithNull() {
        RandSpark randSpark = new RandSpark(new NullLiteral());
        Assertions.assertEquals("rand_spark", randSpark.getName());
        Assertions.assertEquals(1, randSpark.arity());
        Assertions.assertEquals(DoubleType.INSTANCE, randSpark.getDataType());
        Assertions.assertFalse(randSpark.nullable());
    }

    @Test
    public void testSignatures() {
        RandSpark randSpark = new RandSpark();
        Assertions.assertEquals(2, randSpark.getSignatures().size());
    }
}

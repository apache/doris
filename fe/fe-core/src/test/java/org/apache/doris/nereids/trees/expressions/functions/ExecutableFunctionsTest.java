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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.executable.ExecutableFunctions;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

public class ExecutableFunctionsTest {
    @Test
    void testAbsFunctions() {
        TinyIntLiteral tinyInt1 = new TinyIntLiteral((byte) -128);
        Assertions.assertEquals(new SmallIntLiteral((short) 128), ExecutableFunctions.abs(tinyInt1));
        TinyIntLiteral tinyInt2 = new TinyIntLiteral((byte) 127);
        Assertions.assertEquals(new SmallIntLiteral((short) 127), ExecutableFunctions.abs(tinyInt2));

        SmallIntLiteral smallInt1 = new SmallIntLiteral((short) -32768);
        Assertions.assertEquals(new IntegerLiteral(32768), ExecutableFunctions.abs(smallInt1));
        SmallIntLiteral smallInt2 = new SmallIntLiteral((short) 32767);
        Assertions.assertEquals(new IntegerLiteral(32767), ExecutableFunctions.abs(smallInt2));

        IntegerLiteral int1 = new IntegerLiteral(-2147483648);
        Assertions.assertEquals(new BigIntLiteral(2147483648L), ExecutableFunctions.abs(int1));
        IntegerLiteral int2 = new IntegerLiteral(2147483647);
        Assertions.assertEquals(new BigIntLiteral(2147483647L), ExecutableFunctions.abs(int2));

        BigIntLiteral bigInt1 = new BigIntLiteral(-9223372036854775808L);
        Assertions.assertEquals(new LargeIntLiteral(new BigInteger("9223372036854775808")),
                ExecutableFunctions.abs(bigInt1));
        BigIntLiteral bigInt2 = new BigIntLiteral(9223372036854775807L);
        Assertions.assertEquals(new LargeIntLiteral(new BigInteger("9223372036854775807")),
                ExecutableFunctions.abs(bigInt2));

        LargeIntLiteral largeInt1 = new LargeIntLiteral(new BigInteger("-170141183460469231731687303715884105728"));
        Assertions.assertEquals(new LargeIntLiteral(new BigInteger("170141183460469231731687303715884105728")),
                ExecutableFunctions.abs(largeInt1));
        LargeIntLiteral largeInt2 = new LargeIntLiteral(new BigInteger("170141183460469231731687303715884105727"));
        Assertions.assertEquals(new LargeIntLiteral(new BigInteger("170141183460469231731687303715884105727")),
                ExecutableFunctions.abs(largeInt2));
    }
}

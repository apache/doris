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

package org.apache.doris.nereids.types;

import org.junit.Assert;
import org.junit.Test;

public class DataTypeTest {
    @Test
    public void testDataTypeEquals() {
        BigIntType bigIntType1 = new BigIntType();
        BigIntType bigIntType2 = new BigIntType();
        Assert.assertEquals(bigIntType1, bigIntType2);
        Assert.assertEquals(bigIntType1.hashCode(), bigIntType2.hashCode());

        BooleanType booleanType1 = new BooleanType();
        BooleanType booleanType2 = new BooleanType();
        Assert.assertEquals(booleanType1, booleanType2);
        Assert.assertEquals(booleanType1.hashCode(), booleanType2.hashCode());

        DoubleType doubleType1 = new DoubleType();
        DoubleType doubleType2 = new DoubleType();
        Assert.assertEquals(doubleType1, doubleType2);
        Assert.assertEquals(doubleType1.hashCode(), doubleType2.hashCode());

        IntegerType integerType1 = new IntegerType();
        IntegerType integerType2 = new IntegerType();
        Assert.assertEquals(integerType1, integerType2);
        Assert.assertEquals(integerType1.hashCode(), integerType2.hashCode());

        NullType nullType1 = new NullType();
        NullType nullType2 = new NullType();
        Assert.assertEquals(nullType1, nullType2);
        Assert.assertEquals(nullType1.hashCode(), nullType2.hashCode());

        StringType stringType1 = new StringType();
        StringType stringType2 = new StringType();
        Assert.assertEquals(stringType1, stringType2);
        Assert.assertEquals(stringType1.hashCode(), stringType2.hashCode());

        VarcharType varcharType1 = new VarcharType(32);
        VarcharType varcharType2 = new VarcharType(32);
        Assert.assertEquals(varcharType1, varcharType2);
        Assert.assertEquals(varcharType1.hashCode(), varcharType2.hashCode());
        VarcharType varcharType3 = new VarcharType(64);
        Assert.assertNotEquals(varcharType1, varcharType3);
        Assert.assertNotEquals(varcharType1.hashCode(), varcharType3.hashCode());
    }
}

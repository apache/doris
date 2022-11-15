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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataTypeTest {
    @Test
    public void testDataTypeEquals() {
        DecimalV2Type decimalV2Type1 = new DecimalV2Type(27, 9);
        DecimalV2Type decimalV2Type2 = new DecimalV2Type(27, 9);
        DecimalV2Type decimalV2Type3 = new DecimalV2Type(26, 9);
        DecimalV2Type decimalV2Type4 = new DecimalV2Type(27, 8);
        Assertions.assertEquals(decimalV2Type1, decimalV2Type2);
        Assertions.assertEquals(decimalV2Type1.hashCode(), decimalV2Type2.hashCode());
        Assertions.assertNotEquals(decimalV2Type1, decimalV2Type3);
        Assertions.assertNotEquals(decimalV2Type1.hashCode(), decimalV2Type3.hashCode());
        Assertions.assertNotEquals(decimalV2Type1, decimalV2Type4);
        Assertions.assertNotEquals(decimalV2Type1.hashCode(), decimalV2Type4.hashCode());

        CharType charType1 = new CharType(10);
        CharType charType2 = new CharType(10);
        CharType charType3 = new CharType(15);
        Assertions.assertEquals(charType1, charType2);
        Assertions.assertEquals(charType1.hashCode(), charType2.hashCode());
        Assertions.assertNotEquals(charType1, charType3);
        Assertions.assertNotEquals(charType1.hashCode(), charType3.hashCode());

        VarcharType varcharType1 = new VarcharType(32);
        VarcharType varcharType2 = new VarcharType(32);
        VarcharType varcharType3 = new VarcharType(64);
        Assertions.assertEquals(varcharType1, varcharType2);
        Assertions.assertEquals(varcharType1.hashCode(), varcharType2.hashCode());
        Assertions.assertNotEquals(varcharType1, varcharType3);
        Assertions.assertNotEquals(varcharType1.hashCode(), varcharType3.hashCode());
    }
}

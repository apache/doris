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

public class DecimalTypeTest {
    @Test
    public void testIsWiderThan() {
        Assertions.assertTrue(DecimalType.createDecimalType(10, 5)
                .isWiderThan(DecimalType.createDecimalType(8, 4)));
        Assertions.assertFalse(DecimalType.createDecimalType(10, 5)
                .isWiderThan(DecimalType.createDecimalType(8, 6)));
        Assertions.assertFalse(DecimalType.createDecimalType(10, 5)
                .isWiderThan(DecimalType.createDecimalType(8, 2)));
    }

    @Test
    public void testWiderDecimal() {
        Assertions.assertEquals(DecimalType.createDecimalType(14, 5),
                DecimalType.widerDecimalType(
                        DecimalType.createDecimalType(12, 3),
                        DecimalType.createDecimalType(10, 5)));
    }

    @Test
    public void testConstructor() {
        Assertions.assertEquals(DecimalType.createDecimalType(38, 38),
                DecimalType.createDecimalType(39, 39));
    }
}

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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.UuidType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UuidLiteralTest {

    @Test
    void parseCanonicalAndCompactForms() {
        UuidLiteral canonical = new UuidLiteral("550E8400-E29B-41D4-A716-446655440000");
        UuidLiteral compact = new UuidLiteral("550e8400e29b41d4a716446655440000");

        Assertions.assertEquals(UuidType.INSTANCE, canonical.getDataType());
        Assertions.assertEquals("550e8400-e29b-41d4-a716-446655440000", canonical.getValue().toString());
        Assertions.assertEquals(canonical.getValue(), compact.getValue());
        Assertions.assertEquals("550e8400-e29b-41d4-a716-446655440000",
                canonical.toLegacyLiteral().getStringValue());
    }

    @Test
    void rejectInvalidForms() {
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("550e8400-e29b-41d4-a716-44665544000g"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("{550e8400-e29b-41d4-a716-446655440000}"));
        Assertions.assertThrows(AnalysisException.class,
                () -> new UuidLiteral("550e8400e29b-41d4-a716-446655440000"));
    }

    @Test
    void compareAsUnsigned128BitValues() {
        UuidLiteral smaller = new UuidLiteral("00000000-0000-0000-ffff-ffffffffffff");
        UuidLiteral larger = new UuidLiteral("00000000-0000-0001-0000-000000000000");
        UuidLiteral highBit = new UuidLiteral("80000000-0000-0000-0000-000000000000");

        Assertions.assertTrue(smaller.compareTo(larger) < 0);
        Assertions.assertTrue(larger.compareTo(highBit) < 0);
    }
}

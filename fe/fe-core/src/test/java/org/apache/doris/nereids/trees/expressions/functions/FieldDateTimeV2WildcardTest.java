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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Field;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.types.DateTimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for DateTimeV2Type wildcard/MAX handling in function signatures.
 */
public class FieldDateTimeV2WildcardTest {

    @Test
    public void testFieldDateTimeV2WildcardPromotion() {
        // Create literals with different datetimev2 precisions
        DateTimeV2Literal search = new DateTimeV2Literal("2020-02-02 00:00:00.123"); // scale 3
        DateTimeV2Literal v1 = new DateTimeV2Literal("2020-02-02 00:00:00.12"); // scale 2
        DateTimeV2Literal v2 = new DateTimeV2Literal("2020-02-02 00:00:00.1"); // scale 1

        Field field = new Field(search, v1, v2);
        FunctionSignature signature = field.getSignature();

        // The final promoted precision should be max(3,2,1) -> 3
        Assertions.assertEquals(DateTimeV2Type.of(3), signature.getArgType(0));
        Assertions.assertEquals(DateTimeV2Type.of(3), signature.getVarArgType().get());
    }
}

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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StrToDateTest {

    @Test
    void testComputeSignatureWithFoldableFormat() {
        StrToDate dateFormat = new StrToDate(new StringLiteral("2024-01-01"),
                new Concat(new StringLiteral("%Y-%m"), new StringLiteral("-%d")));
        StrToDate dateTimeFormat = new StrToDate(new StringLiteral("2024-01-01 12:34:56"),
                new Concat(new StringLiteral("%Y-%m-%d"), new StringLiteral(" %H:%i:%s")));

        FunctionSignature dateSignature = dateFormat.computeSignature(StrToDate.SIGNATURES.get(0));
        FunctionSignature dateTimeSignature = dateTimeFormat.computeSignature(StrToDate.SIGNATURES.get(0));

        Assertions.assertEquals(DateV2Type.INSTANCE, dateSignature.returnType);
        Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, dateTimeSignature.returnType);
    }
}

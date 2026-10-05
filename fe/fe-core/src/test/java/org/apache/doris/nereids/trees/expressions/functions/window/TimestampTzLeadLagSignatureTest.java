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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.TimeStampTzType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests ensuring LEAD/LAG select TimeStampTzType signatures when input is TIMESTAMPTZ.
 */
public class TimestampTzLeadLagSignatureTest {

    @Test
    public void testLeadSignatureMatchesTimeStampTz() {
        SlotReference slot = SlotReference.of("ts", TimeStampTzType.of(6));
        Lead lead = new Lead(slot);
        FunctionSignature signature = lead.getSignature();

        Assertions.assertTrue(signature.returnType instanceof TimeStampTzType,
                "LEAD return type should be TimeStampTzType for TIMESTAMPTZ input");
        Assertions.assertTrue(signature.getArgType(0) instanceof TimeStampTzType,
                "LEAD first arg type should be TimeStampTzType for TIMESTAMPTZ input");
    }

    @Test
    public void testLagSignatureMatchesTimeStampTz() {
        SlotReference slot = SlotReference.of("ts", TimeStampTzType.of(6));
        Lag lag = new Lag(slot);
        FunctionSignature signature = lag.getSignature();

        Assertions.assertTrue(signature.returnType instanceof TimeStampTzType,
                "LAG return type should be TimeStampTzType for TIMESTAMPTZ input");
        Assertions.assertTrue(signature.getArgType(0) instanceof TimeStampTzType,
                "LAG first arg type should be TimeStampTzType for TIMESTAMPTZ input");
    }
}

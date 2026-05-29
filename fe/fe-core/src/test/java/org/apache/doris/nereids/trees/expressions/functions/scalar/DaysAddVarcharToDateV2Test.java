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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DaysAddVarcharToDateV2Test {

    @BeforeEach
    public void setUp() {
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testVarcharDirectInputUsesDateV2InPrestoDialect() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysAdd daysAdd = new DaysAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastFromVarcharToDateTimeV2PreservedInPrestoDialect() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        // Non-explicit Cast from VARCHAR to DateTimeV2 should NOT override —
        // the crash fix from 83138ffaf3b
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.WILDCARD, false);
        DaysAdd daysAdd = new DaysAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Falls through to default: DateTimeV2 signature
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testExplicitCastFromVarcharPreservedInPrestoDialect() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeV2Type.WILDCARD, true);
        DaysAdd daysAdd = new DaysAdd(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonExplicitCastToDateV2UsesDateV2SignatureForDaysAdd() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        DaysAdd daysAdd = new DaysAdd(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubVarcharDirectInputUsesDateV2() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysSub daysSub = new DaysSub(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubNonExplicitCastFromVarcharToDateTimeV2Preserved() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateTimeV2Type.WILDCARD, false);
        DaysSub daysSub = new DaysSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubExplicitCastFromVarcharPreserved() {
        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast explicitCast = new Cast(varcharColumn, DateTimeV2Type.WILDCARD, true);
        DaysSub daysSub = new DaysSub(explicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubNonExplicitCastToDateV2UsesDateV2Signature() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("presto");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        Cast implicitCast = new Cast(varcharColumn, DateV2Type.INSTANCE, false);
        DaysSub daysSub = new DaysSub(implicitCast, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        Assertions.assertEquals(DateV2Type.INSTANCE, signature.returnType);
        Assertions.assertEquals(DateV2Type.INSTANCE, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testDaysSubDefaultSignatureComputation() {
        SlotReference dateTimeColumn = new SlotReference("dt", DateTimeV2Type.WILDCARD);
        DaysSub daysSub = new DaysSub(dateTimeColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysSub.getSignature();

        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.returnType);
        Assertions.assertEquals(DateTimeV2Type.WILDCARD, signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }

    @Test
    public void testNonPrestoDialectUsesDefaultSignature() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlDialect("doris");
        context.setThreadLocalInfo();

        SlotReference varcharColumn = new SlotReference("cal_dt", VarcharType.SYSTEM_DEFAULT);
        DaysAdd daysAdd = new DaysAdd(varcharColumn, new IntegerLiteral(1));

        FunctionSignature signature = daysAdd.getSignature();

        // Default: matches DateTimeV2 signature (first in SIGNATURES list)
        Assertions.assertTrue(signature.returnType instanceof DateTimeV2Type,
                "Expected DateTimeV2 return type but got " + signature.returnType);
        Assertions.assertTrue(signature.argumentsTypes.get(0) instanceof DateTimeV2Type,
                "Expected DateTimeV2 arg type but got " + signature.argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signature.argumentsTypes.get(1));
    }
}

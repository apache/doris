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

package org.apache.doris.catalog;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

class FunctionNameTest {
    @Test
    void testUnqualifiedFunctionHashCode() {
        FunctionName name = new FunctionName("date_trunc");
        FunctionName sameName = new FunctionName(null, "DATE_TRUNC");
        FunctionName builtinName = FunctionName.createBuiltinName("DATE_TRUNC");
        Assertions.assertEquals(name, sameName);
        Assertions.assertEquals(name, builtinName);
        Assertions.assertEquals(name.hashCode(), sameName.hashCode());
        Assertions.assertEquals(name.hashCode(), builtinName.hashCode());
        Set<FunctionName> names = new HashSet<>();
        names.add(name);
        Assertions.assertTrue(names.contains(sameName));
        Assertions.assertTrue(names.contains(builtinName));
        Assertions.assertFalse(names.contains(new FunctionName("abs")));
    }

    @Test
    void testQualifiedFunctionHashCode() {
        FunctionName name = new FunctionName("test_db", "my_func");
        FunctionName sameName = new FunctionName("TEST_DB", "MY_FUNC");
        Assertions.assertEquals(name, sameName);
        Assertions.assertEquals(name.hashCode(), sameName.hashCode());
        Assertions.assertEquals(31 * "test_db".hashCode() + "my_func".hashCode(), name.hashCode());
        Assertions.assertNotEquals(name, new FunctionName("my_func"));
        Assertions.assertNotEquals(new FunctionName("my_func"), name);
    }

    @Test
    void testFunctionCallExprHashCode() {
        FunctionCallExpr expr = dateTruncExpr();
        FunctionCallExpr sameExpr = dateTruncExpr();
        Assertions.assertEquals(expr, sameExpr);
        Assertions.assertEquals(expr.hashCode(), sameExpr.hashCode());
    }

    @Test
    void testMvPartitionInfoDiagnosticFormatting() {
        MTMVPartitionInfo partitionInfo = new MTMVPartitionInfo(MTMVPartitionType.EXPR, "data_date");
        partitionInfo.setExpr(dateTruncExpr());
        String diagnostic = Assertions.assertDoesNotThrow(
                () -> String.format("mv partition info is %s", partitionInfo));
        Assertions.assertTrue(diagnostic.startsWith("mv partition info is " + MTMVPartitionInfo.class.getName()));
    }

    private FunctionCallExpr dateTruncExpr() {
        return new FunctionCallExpr("date_trunc", ImmutableList.of(
                new SlotRef(null, "data_date"), new StringLiteral("day")), true);
    }
}

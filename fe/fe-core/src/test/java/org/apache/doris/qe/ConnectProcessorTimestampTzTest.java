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

package org.apache.doris.qe;

import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.thrift.TExprNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Method;

class ConnectProcessorTimestampTzTest {
    @ParameterizedTest
    @CsvSource({
            "0, 2024-01-01 00:00:00+00:00",
            "6, 2024-01-01 00:00:00.000000+00:00",
            "3, 2024-01-01 00:00:00.120+00:00",
            "6, 2024-01-01 00:00:00.123456+00:00"
    })
    void testForwardedTimestampTzKeepsTypeScaleAndValue(int scale, String value) throws Exception {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("+08:00");
        context.setThreadLocalInfo();
        try {
            LiteralExpr literal = new TimestampTzLiteral(TimeStampTzType.of(scale), value).toLegacyLiteral();
            TExprNode node = ExprToThriftVisitor.treeToThrift(literal).getNodes().get(0);
            Method deserialize = ConnectProcessor.class.getDeclaredMethod(
                    "getLiteralExprFromThrift", TExprNode.class);
            deserialize.setAccessible(true);

            // A UTC midnight value must keep its time zone type, even when its fractional part is zero.
            // The declared scale must also survive. For example, .120 has scale 3, not scale 2.
            LiteralExpr restored = (LiteralExpr) deserialize.invoke(null, node);
            Assertions.assertEquals(literal.getType(), restored.getType());
            Assertions.assertEquals(value, restored.getStringValue());
        } finally {
            ConnectContext.remove();
        }
    }
}

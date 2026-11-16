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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TExprNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.stream.Stream;

class ConnectProcessorDateLiteralTest {
    private static Stream<Arguments> dateLiterals() {
        LocalDateTime midnight = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime noon = midnight.withHour(12);
        return Stream.of(
                new DateLiteral(midnight, Type.DATE),
                new DateLiteral(midnight, Type.DATEV2),
                new DateLiteral(midnight, Type.DATETIME),
                new DateLiteral(noon, Type.DATETIME),
                new DateLiteral(midnight, ScalarType.createDatetimeV2Type(0)),
                new DateLiteral(midnight, ScalarType.createDatetimeV2Type(6)),
                new DateLiteral(noon, ScalarType.createDatetimeV2Type(6)),
                new DateLiteral(noon.withNano(120000000), ScalarType.createDatetimeV2Type(3)),
                new DateLiteral(noon.withNano(120000000), ScalarType.createDatetimeV2Type(6)),
                new DateLiteral(noon.withNano(123456000), ScalarType.createDatetimeV2Type(6)))
                .flatMap(literal -> Stream.of(Arguments.of(literal, true), Arguments.of(literal, false)));
    }

    @ParameterizedTest
    @MethodSource("dateLiterals")
    void testForwardedDateKeepsTypeScaleAndValue(DateLiteral literal, boolean enableDateConversion) throws Exception {
        boolean originalDateConversion = Config.enable_date_conversion;
        try {
            // The receiver must use the sender's type, even when its default date type differs.
            Config.enable_date_conversion = enableDateConversion;
            TExprNode node = ExprToThriftVisitor.treeToThrift(literal).getNodes().get(0);
            Method deserialize = ConnectProcessor.class.getDeclaredMethod(
                    "getLiteralExprFromThrift", TExprNode.class);
            deserialize.setAccessible(true);

            LiteralExpr restored = (LiteralExpr) deserialize.invoke(null, node);
            Assertions.assertEquals(literal.getType(), restored.getType());
            Assertions.assertEquals(literal.getStringValue(), restored.getStringValue());
        } finally {
            Config.enable_date_conversion = originalDateConversion;
        }
    }
}

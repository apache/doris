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

package org.apache.doris.analysis;

import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.algebra.Window;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;

class AnalyticWindowTest {
    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 2147483646L, Integer.MAX_VALUE})
    void testNereidsRowsOffsetWithinMaxIntIsSerialized(long offset) {
        Window window = Collections::emptyList;
        for (Literal literal : new Literal[] {new BigIntLiteral(offset),
                new LargeIntLiteral(BigInteger.valueOf(offset))}) {
            for (FrameBoundary frameBoundary : new FrameBoundary[] {
                    FrameBoundary.newPrecedingBoundary(literal), FrameBoundary.newFollowingBoundary(literal)}) {
                AnalyticWindow.Boundary boundary = window.withFrameBoundary(frameBoundary, null);
                Assertions.assertEquals(offset,
                        boundary.toThrift(AnalyticWindow.Type.ROWS).getRowsOffsetValue());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"2147483648", "9223372036854775805", "9223372036854775806",
            "9223372036854775807", "9223372036854775808"})
    void testRowsOffsetOverMaxIntIsRejected(String offset) {
        for (AnalyticWindow.BoundaryType boundaryType : new AnalyticWindow.BoundaryType[] {
                AnalyticWindow.BoundaryType.PRECEDING, AnalyticWindow.BoundaryType.FOLLOWING}) {
            AnalyticWindow.Boundary boundary = new AnalyticWindow.Boundary(
                    boundaryType, new IntLiteral(1L), new BigDecimal(offset));

            IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                    () -> boundary.toThrift(AnalyticWindow.Type.ROWS));
            Assertions.assertEquals("ROWS window offset must not exceed " + Integer.MAX_VALUE,
                    exception.getMessage());
        }
    }
}

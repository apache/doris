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
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.plans.algebra.Window;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;

class AnalyticWindowTest {
    @Test
    void testNereidsRowsOffsetKeepsInt64Precision() {
        Window window = Collections::emptyList;
        FrameBoundary frameBoundary = FrameBoundary.newPrecedingBoundary(
                new LargeIntLiteral(BigInteger.valueOf(Long.MAX_VALUE)));

        AnalyticWindow.Boundary boundary = window.withFrameBoundary(frameBoundary, null);

        Assertions.assertEquals(Long.MAX_VALUE,
                boundary.toThrift(AnalyticWindow.Type.ROWS).getRowsOffsetValue());
    }

    @Test
    void testRowsOffsetOverMaxInt64IsRejected() {
        BigDecimal offset = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
        AnalyticWindow.Boundary boundary = new AnalyticWindow.Boundary(
                AnalyticWindow.BoundaryType.PRECEDING, new IntLiteral(1L), offset);

        Assertions.assertThrows(IllegalStateException.class,
                () -> boundary.toThrift(AnalyticWindow.Type.ROWS));
    }
}

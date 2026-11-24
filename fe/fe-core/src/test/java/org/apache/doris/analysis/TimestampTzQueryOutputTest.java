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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.foundation.format.FormatOptions;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class TimestampTzQueryOutputTest {
    @Test
    public void testDateLiteralTimeStampTzHistoricalOffsetStreamLoad() throws Exception {
        // Query output preserves historical offsets; storage input remains canonical UTC.
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("Asia/Shanghai");
            // Pre-standard-offset instant: 1900-01-01 00:00:00 UTC.
            DateLiteral d = DateLiteralUtils.createDateLiteral("1900-01-01 00:00:00+00:00",
                    ScalarType.createTimeStampTzType(0));
            // Stream-load path must emit UTC format.
            Assertions.assertEquals("1900-01-01 00:00:00+00:00",
                    d.getStringValueForStreamLoad(FormatOptions.getDefault()));
            Assertions.assertEquals("1900-01-01 08:05:43+08:05:43",
                    d.getStringValueForQuery(FormatOptions.getDefault()));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testTimeStampTzQueryRejectsLocalYearOverflow() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            for (int scale : new int[] {0, 3, 6}) {
                String fraction = scale == 0 ? "" : "." + String.join("", Collections.nCopies(scale, "9"));
                for (boolean upper : new boolean[] {false, true}) {
                    String zone = upper ? "+08:00" : "-08:00";
                    context.getSessionVariable().setTimeZone(zone);
                    DateLiteral literal = DateLiteralUtils.createDateLiteral(upper
                                    ? "9999-12-31 23:59:59" + fraction + "+00:00"
                                    : "0000-01-01 00:00:00+00:00",
                            ScalarType.createTimeStampTzType(scale));
                    for (boolean nested : new boolean[] {false, true}) {
                        FormatOptions options = FormatOptions.getDefault();
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                () -> {
                                    if (nested) {
                                        literal.getStringValueInComplexTypeForQuery(options);
                                    } else {
                                        literal.getStringValueForQuery(options);
                                    }
                                });
                        Assertions.assertTrue(error.getMessage().contains(
                                "TIMESTAMPTZ local year is outside [0, 9999]: year=" + (upper ? 10000 : -1)));
                    }
                }
            }
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testTimeStampTzStreamLoadKeepsUtcBounds() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            for (String zone : new String[] {"+08:00", "-08:00"}) {
                context.getSessionVariable().setTimeZone(zone);
                for (String text : new String[] {"0000-01-01 00:00:00.000000+00:00",
                        "9999-12-31 23:59:59.999999+00:00"}) {
                    DateLiteral literal = DateLiteralUtils.createDateLiteral(text, ScalarType.createTimeStampTzType(6));
                    Assertions.assertEquals(text, literal.getStringValueForStreamLoad(FormatOptions.getDefault()));
                    Assertions.assertEquals("\"" + text + "\"", literal.getStringValueInComplexTypeForQuery(
                            FormatOptions.getDefault().forStreamLoad()));
                }
            }
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testTimeStampTzQueryLocalBounds() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("+08:00");
            DateLiteral maximum = DateLiteralUtils.createDateLiteral("9999-12-31 15:59:59.999999+00:00",
                    ScalarType.createTimeStampTzType(6));
            Assertions.assertEquals("9999-12-31 23:59:59.999999+08:00",
                    maximum.getStringValueForQuery(FormatOptions.getDefault()));
            context.getSessionVariable().setTimeZone("-08:00");
            DateLiteral minimum = DateLiteralUtils.createDateLiteral("0000-01-01 08:00:00+00:00",
                    ScalarType.createTimeStampTzType(6));
            Assertions.assertEquals("0000-01-01 00:00:00.000000-08:00",
                    minimum.getStringValueForQuery(FormatOptions.getDefault()));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testNestedStoragePreservesUtcAndQueryOptions() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            context.getSessionVariable().setTimeZone("+08:00");
            String text = "9999-12-31 23:59:59.999999+00:00";
            DateLiteral literal = DateLiteralUtils.createDateLiteral(text, ScalarType.createTimeStampTzType(6));
            ArrayLiteral array = new ArrayLiteral(new ArrayType(literal.getType()), literal);
            for (LiteralExpr container : ImmutableList.of(array,
                    new MapLiteral(new MapType(Type.INT, literal.getType()),
                            ImmutableList.of(new IntLiteral(1)), ImmutableList.of(literal)),
                    new StructLiteral(literal), new StructLiteral(array))) {
                FormatOptions options = FormatOptions.getDefault();
                Assertions.assertTrue(container.getStringValueForStreamLoad(options).contains(text));
                Assertions.assertFalse(options.isForStreamLoad());
                Assertions.assertThrows(AnalysisException.class, () -> container.getStringValueForQuery(options));
            }
        } finally {
            ConnectContext.remove();
        }
    }
}

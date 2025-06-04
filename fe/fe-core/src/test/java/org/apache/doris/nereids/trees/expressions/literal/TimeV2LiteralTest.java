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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.TimeV2Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeV2LiteralTest {

    @Test
    public void testTimeV2LiteralCreate() {
        // without micro second
        TimeV2Literal literal = new TimeV2Literal(TimeV2Type.of(0), "12:12:12");
        String s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12");
        // max val
        literal = new TimeV2Literal(TimeV2Type.of(0), "838:59:59");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59");
        // min val
        literal = new TimeV2Literal(TimeV2Type.of(0), "-838:59:59");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59");
        // hour is negative
        literal = new TimeV2Literal(TimeV2Type.of(0), "-00:01:01");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-00:01:01");
        literal = new TimeV2Literal(-3599000000.0);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-00:59:59.000000");
        // contail micro second part
        literal = new TimeV2Literal(TimeV2Type.of(1), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.1");
        literal = new TimeV2Literal(TimeV2Type.of(2), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.12");
        literal = new TimeV2Literal(TimeV2Type.of(3), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121");
        literal = new TimeV2Literal(TimeV2Type.of(4), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.1212");
        literal = new TimeV2Literal(TimeV2Type.of(5), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.12121");
        literal = new TimeV2Literal(TimeV2Type.of(6), "12:12:12.121212");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121212");
        // max val
        literal = new TimeV2Literal(TimeV2Type.of(6), "838:59:59.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        // min val
        literal = new TimeV2Literal(TimeV2Type.of(6), "-838:59:59.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // not string
        literal = new TimeV2Literal(12, 12, 12, 121212, 6, false);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:12:12.121212");
        // max val
        literal = new TimeV2Literal(838, 59, 59, 999999, 6, false);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        // min val
        literal = new TimeV2Literal(838, 59, 59, 999999, 6, true);
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // string without ":"
        literal = new TimeV2Literal(TimeV2Type.of(0), "8385959");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59");
        literal = new TimeV2Literal(TimeV2Type.of(0), "-8385959");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59");
        literal = new TimeV2Literal(TimeV2Type.of(0), "120000");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:00:00");
        literal = new TimeV2Literal(TimeV2Type.of(6), "8385959.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "838:59:59.999999");
        literal = new TimeV2Literal(TimeV2Type.of(6), "-8385959.999999");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "-838:59:59.999999");
        // one ":"
        literal = new TimeV2Literal(TimeV2Type.of(0), "12:00");
        s = literal.getStringValue();
        Assertions.assertEquals(s, "12:00:00");
    }

    @Test
    public void testTimeV2LiteralOutOfRange() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 59, 1000000, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 60, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 60, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(839, 59, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, 59, -1, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, 59, -1, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(838, -1, 59, 999999, 6, false);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(839, 59, 59, 999999, 6, true);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(3020400000000.0);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(-3020400000000.0);
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "838:59:60");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "838:60:59");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "839:59:59");
        });
        Assertions.assertThrows(AnalysisException.class, () -> {
            new TimeV2Literal(TimeV2Type.of(0), "-839:59:59");
        });
    }

}

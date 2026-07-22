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

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for DateFormat function.
 * This test verifies that DateFormat correctly implements AlwaysNullable interface
 * and returns nullable type even when input arguments are not nullable.
 */
class DateFormatTest extends SqlTestBase {

    /**
     * Test that DateFormat implements AlwaysNullable interface.
     * The function should always return nullable type, even when input is not nullable.
     */
    @Test
    void testAlwaysNullableInterface() {
        // Create a DateFormat function with non-nullable inputs
        DateTimeV2Literal datetime = new DateTimeV2Literal("2024-01-01 12:00:00");
        StringLiteral format = new StringLiteral("%Y-%m-%d");
        DateFormat dateFormat = new DateFormat(datetime, format);

        // DateFormat implements AlwaysNullable, so it should always be nullable
        Assertions.assertTrue(dateFormat.nullable(),
                "DateFormat should be nullable because it implements AlwaysNullable interface");
    }

    /**
     * Test that DateFormat works correctly with SQL analysis.
     */
    @Test
    void testDateFormatWithLiteral() {
        String sql = "SELECT date_format(cast('2024-01-01 12:00:00' as datetimev2), '%Y-%m-%d')";

        // Analyze should succeed without throwing exception
        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext).analyze(sql);
        }, "date_format should work correctly with datetimev2 literal");
    }

    /**
     * Test that DateFormat works correctly with NULL input.
     */
    @Test
    void testNullInput() {
        String sql = "SELECT date_format(NULL, '%Y-%m-%d')";

        // Analyze should succeed without throwing exception
        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext).analyze(sql);
        }, "date_format should handle NULL input correctly");
    }

    /**
     * Test that DateFormat function signature is correct.
     */
    @Test
    void testFunctionSignature() {
        // Create DateFormat and verify it has the correct signature
        DateTimeV2Literal datetime = new DateTimeV2Literal("2024-01-01 12:00:00");
        StringLiteral format = new StringLiteral("%Y-%m-%d");
        DateFormat dateFormat = new DateFormat(datetime, format);

        // Verify the function has correct children
        Assertions.assertEquals(2, dateFormat.arity(), "DateFormat should have 2 arguments");
        Assertions.assertEquals(datetime, dateFormat.child(0), "First child should be datetime");
        Assertions.assertEquals(format, dateFormat.child(1), "Second child should be format string");
    }
}

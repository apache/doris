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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.types.TimeStampTzType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

public class ColumnDefinitionTest {

    @Test
    public void testNameEquals() {
        ColumnDefinition columnDefinition = new ColumnDefinition("col1", null, false, null, false, null, null);
        String otherColName = "col1";
        boolean expected = true;
        Assertions.assertEquals(expected, columnDefinition.nameEquals(otherColName, false));

        String otherColName2 = "col2";
        boolean expected2 = false;
        Assertions.assertEquals(expected2, columnDefinition.nameEquals(otherColName2, false));
    }

    @Test
    public void testTranslateToCatalogStyleForSchemaChangeWithTimeStampTzCurrentTimestamp() {
        ColumnDefinition columnDefinition = new ColumnDefinition("ts", TimeStampTzType.of(6), false,
                null, true, Optional.of(DefaultValue.currentTimeStampDefaultValueWithPrecision(6L)), "");

        Column column = columnDefinition.translateToCatalogStyleForSchemaChange();

        Assertions.assertEquals("CURRENT_TIMESTAMP(6)", column.getDefaultValue());
        Assertions.assertNotNull(column.getRealDefaultValue());
        Assertions.assertTrue(column.getRealDefaultValue().matches(
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}[+-]\\d{2}:\\d{2}"));
    }

    @Test
    public void testTimestampTzFormatterUsesNumericUtcOffset() {
        ZonedDateTime utcDateTime = ZonedDateTime.of(2026, 5, 16, 12, 0, 0, 123456000, ZoneOffset.UTC);

        Assertions.assertEquals("2026-05-16 12:00:00.123456+00:00",
                TimestampTzLiteral.formatDateTime(utcDateTime, 6));
    }
}

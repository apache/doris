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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.TimeStampNsType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CurrentTimestampDefaultValueTest {
    @Test
    void testTimestampNsCurrentTimestampDefault() {
        Assertions.assertDoesNotThrow(
                () -> ColumnDef.validateDefaultValue(
                        Type.TIMESTAMP_NS, "CURRENT_TIMESTAMP",
                        new DefaultValueExprDef(ColumnDef.DefaultValue.NOW)));
        Assertions.assertDoesNotThrow(
                () -> ColumnDef.validateDefaultValue(
                        Type.TIMESTAMP_NS, "CURRENT_TIMESTAMP(9)",
                        new DefaultValueExprDef(ColumnDef.DefaultValue.NOW, 9L)));
    }

    @Test
    void testCurrentTimestampKeepsMicrosecondPrecision() {
        for (long precision = 1; precision <= 9; precision++) {
            String suffix = "\\.\\d{" + precision + "}";
            String pattern = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}" + suffix;

            ColumnDef.DefaultValue legacy = new ColumnDef.DefaultValue(
                    true, "CURRENT_TIMESTAMP(" + precision + ")", ColumnDef.DefaultValue.NOW, precision);
            Assertions.assertTrue(legacy.getValue().matches(pattern));

            if (precision <= 6) {
                org.apache.doris.nereids.trees.plans.commands.info.DefaultValue datetimeV2 =
                        org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                                .currentTimeStampDefaultValueWithPrecision(precision);
                Assertions.assertTrue(datetimeV2.getRawValue().matches(pattern));
            }
            org.apache.doris.nereids.trees.plans.commands.info.DefaultValue timestampNs =
                    org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(precision, TimeStampNsType.INSTANCE);
            Assertions.assertTrue(timestampNs.getRawValue().matches(pattern));
        }

        for (long precision = 7; precision <= 9; precision++) {
            long invalidPrecision = precision;
            Assertions.assertThrows(AnalysisException.class,
                    () -> org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(invalidPrecision));
        }
        Assertions.assertThrows(AnalysisException.class,
                () -> org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                        .currentTimeStampDefaultValueWithPrecision(10L, TimeStampNsType.INSTANCE));
    }
}

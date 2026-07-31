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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CurrentTimestampDefaultValueTest {
    @Test
    void testTimestampNsCurrentTimestampDefault() {
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(
                        Type.TIMESTAMP_NS, "CURRENT_TIMESTAMP",
                        new DefaultValueExprDef(ColumnDef.DefaultValue.NOW)));
        Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(
                        Type.TIMESTAMP_NS, "CURRENT_TIMESTAMP(6)",
                        new DefaultValueExprDef(ColumnDef.DefaultValue.NOW, 6L)));
    }

    @Test
    void testCurrentTimestampKeepsMicrosecondPrecision() {
        for (long precision = 1; precision <= 6; precision++) {
            String suffix = "\\.\\d{" + precision + "}";
            String pattern = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}" + suffix;

            ColumnDef.DefaultValue legacy = new ColumnDef.DefaultValue(
                    true, "CURRENT_TIMESTAMP(" + precision + ")", ColumnDef.DefaultValue.NOW, precision);
            Assertions.assertTrue(legacy.getValue().matches(pattern));

            org.apache.doris.nereids.trees.plans.commands.info.DefaultValue nereids =
                    org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(precision);
            Assertions.assertTrue(nereids.getRawValue().matches(pattern));
        }

        for (long precision = 7; precision <= 9; precision++) {
            long invalidPrecision = precision;
            Assertions.assertThrows(AnalysisException.class,
                    () -> org.apache.doris.nereids.trees.plans.commands.info.DefaultValue
                            .currentTimeStampDefaultValueWithPrecision(invalidPrecision));
        }
    }
}

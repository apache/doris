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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergTimestampMappingTest {
    @Test
    void legacyFlagsCannotChangeNestedPhysicalTypes() {
        for (boolean flag : new boolean[] {false, true}) {
            Assertions.assertEquals(ConnectorType.of("DATETIMEV2", 6, 0), IcebergTypeMapping.fromIcebergType(
                    org.apache.iceberg.types.Types.TimestampType.withoutZone(), flag, flag));
            ConnectorType nested = IcebergTypeMapping.fromIcebergType(
                    org.apache.iceberg.types.Types.ListType.ofOptional(1,
                            org.apache.iceberg.types.Types.TimestampType.withZone()), flag, flag);
            Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0), nested.getChildren().get(0));
            Assertions.assertEquals("VARBINARY", IcebergTypeMapping.fromIcebergType(
                    org.apache.iceberg.types.Types.FixedType.ofLength(16), flag, flag).getTypeName());
        }
    }

    @Test
    void instantPredicateDoesNotApplySessionZoneTwice() {
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.optional(1, "ts",
                        org.apache.iceberg.types.Types.TimestampType.withZone()));
        ConnectorType type = ConnectorType.of("TIMESTAMPTZ", 6, 0);
        // TIMESTAMPTZ literals already carry UTC fields, including both instants of a New York DST overlap.
        for (java.time.LocalDateTime utc : java.util.Arrays.asList(
                java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
                java.time.LocalDateTime.of(2021, 11, 7, 5, 30, 0, 123456000),
                java.time.LocalDateTime.of(2021, 11, 7, 6, 30, 0, 123456000))) {
            long micros = utc.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L + utc.getNano() / 1000;
            org.apache.doris.connector.spi.pushdown.ConnectorComparison filter =
                    new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                            org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.GE,
                            new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", type),
                            new org.apache.doris.connector.spi.pushdown.ConnectorLiteral(type, utc));
            for (String zone : java.util.Arrays.asList("UTC", "Asia/Shanghai", "America/New_York")) {
                for (IcebergPredicateConverter.Mode mode : IcebergPredicateConverter.Mode.values()) {
                    org.apache.iceberg.expressions.Expression expression = new IcebergPredicateConverter(
                            schema, java.time.ZoneId.of(zone), mode).convert(filter).get(0);
                    Assertions.assertEquals(micros,
                            ((org.apache.iceberg.expressions.UnboundPredicate<?>) expression).literal().value(),
                            utc + " " + zone + " " + mode);
                }
            }
        }
    }

}

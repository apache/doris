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

package org.apache.doris.connector.trino;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TrinoTimestampMappingTest {
    @Test
    void preserveTimestampKindAndCapPrecision() {
        Assertions.assertEquals("TIMESTAMPTZ", TrinoTypeMapping.toConnectorType(
                io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)).getTypeName());
        Assertions.assertEquals(6, TrinoTypeMapping.toConnectorType(
                io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)).getPrecision());
        Assertions.assertEquals("DATETIMEV2", TrinoTypeMapping.toConnectorType(
                io.trino.spi.type.TimestampType.createTimestampType(9)).getTypeName());
        Assertions.assertEquals("VARBINARY", TrinoTypeMapping.toConnectorType(
                io.trino.spi.type.VarbinaryType.VARBINARY).getTypeName());
    }

    @Test
    void zonedDomainsPreserveEpochFractionsAndRejectLossyBounds() {
        io.trino.spi.connector.ColumnHandle handle = new io.trino.spi.connector.ColumnHandle() { };
        org.apache.doris.connector.spi.ConnectorType dorisType =
                org.apache.doris.connector.spi.ConnectorType.of("TIMESTAMPTZ", 6, 0);
        java.time.LocalDateTime utc = java.time.LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000);
        org.apache.doris.connector.spi.pushdown.ConnectorExpression filter =
                new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                        org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.EQ,
                        new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", dorisType),
                        new org.apache.doris.connector.spi.pushdown.ConnectorLiteral(dorisType, utc));
        for (int precision : new int[] {3, 6, 9}) {
            io.trino.spi.type.Type type =
                    io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision);
            TrinoPredicateConverter converter = new TrinoPredicateConverter(
                    java.util.Collections.singletonMap("ts", handle),
                    java.util.Collections.singletonMap("ts", new io.trino.spi.connector.ColumnMetadata("ts", type)));
            io.trino.spi.predicate.TupleDomain<io.trino.spi.connector.ColumnHandle> domain = converter.convert(filter);
            if (precision == 3 || precision > 6) {
                Assertions.assertTrue(domain.isAll());
            } else {
                Object value = domain.getDomains().orElseThrow(AssertionError::new).get(handle).getSingleValue();
                Assertions.assertEquals(io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        -1L, 999_000_000, io.trino.spi.type.TimeZoneKey.UTC_KEY), value);
            }
        }
    }

    @Test
    void subMicrosecondLocalTimestampDomainStaysLocal() {
        io.trino.spi.connector.ColumnHandle handle = new io.trino.spi.connector.ColumnHandle() { };
        org.apache.doris.connector.spi.ConnectorType dorisType =
                org.apache.doris.connector.spi.ConnectorType.of("DATETIMEV2", 6, 0);
        org.apache.doris.connector.spi.pushdown.ConnectorExpression filter =
                new org.apache.doris.connector.spi.pushdown.ConnectorComparison(
                        org.apache.doris.connector.spi.pushdown.ConnectorComparison.Operator.EQ,
                        new org.apache.doris.connector.spi.pushdown.ConnectorColumnRef("ts", dorisType),
                        new org.apache.doris.connector.spi.pushdown.ConnectorLiteral(dorisType,
                                java.time.LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123456000)));
        // A remote value with nanoseconds 123456789 also decodes to this microsecond literal.
        TrinoPredicateConverter converter = new TrinoPredicateConverter(
                java.util.Collections.singletonMap("ts", handle),
                java.util.Collections.singletonMap("ts", new io.trino.spi.connector.ColumnMetadata(
                        "ts", io.trino.spi.type.TimestampType.createTimestampType(9))));
        Assertions.assertTrue(converter.convert(filter).isAll());
    }

}

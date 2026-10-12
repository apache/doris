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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Collections;

public class IcebergTimestampNsTest {
    @Test
    public void nanosecondCalendarPartitionsDoNotBuildOutOfRangeMvBoundaries() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withoutZone()));

        for (PartitionSpec spec : new PartitionSpec[] {
                PartitionSpec.builderFor(schema).year("ts").build(),
                PartitionSpec.builderFor(schema).month("ts").build(),
                PartitionSpec.builderFor(schema).day("ts").build(),
                PartitionSpec.builderFor(schema).hour("ts").build()}) {
            InMemoryCatalog catalog = new InMemoryCatalog();
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "t"), schema, spec,
                    Collections.singletonMap("format-version", "3"));
            Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(table));
        }
    }

    @Test
    public void nanosecondTimestampRoundTripsWithoutTimezone() {
        ConnectorType mapped = IcebergTypeMapping.fromIcebergType(
                Types.TimestampNanoType.withoutZone(), false, false);
        Assertions.assertEquals("TIMESTAMP_NS", mapped.getTypeName());
        Assertions.assertEquals(Types.TimestampNanoType.withoutZone(),
                IcebergTypeMapping.toIcebergPrimitive(ConnectorType.of("TIMESTAMP_NS")));
        Assertions.assertEquals("UNSUPPORTED", IcebergTypeMapping.fromIcebergType(
                Types.TimestampNanoType.withZone(), false, false).getTypeName());
    }

    @Test
    public void identityPartitionsPreserveNanosecondsAcrossEpochAndTimezones() {
        for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
            for (long nanos : new long[] {Long.MIN_VALUE, -1, 0, 1, 123456789, Long.MAX_VALUE}) {
                String text = IcebergPartitionUtils.serializePartitionValue(
                        Types.TimestampNanoType.withoutZone(), nanos, ZoneId.of(zone));
                Assertions.assertEquals(nanos, IcebergPartitionUtils.parsePartitionValueFromString(
                        text, Types.TimestampNanoType.withoutZone(), ZoneId.of(zone)));
            }
        }
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                IcebergPartitionUtils.parsePartitionValueFromString("2262-04-11 23:47:16.854775808",
                        Types.TimestampNanoType.withoutZone(), ZoneId.of("UTC")));
    }
}

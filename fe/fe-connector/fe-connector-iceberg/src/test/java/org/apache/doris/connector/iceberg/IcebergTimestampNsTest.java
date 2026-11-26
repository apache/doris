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
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class IcebergTimestampNsTest {
    @Test
    public void droppedHistoricalPartitionSourceDoesNotBreakMetadata() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
        Table table = catalog.createTable(TableIdentifier.of("db", "dropped_source"), schema,
                PartitionSpec.builderFor(schema).day("ts").build(),
                Collections.singletonMap("format-version", "1"));
        table.updateSpec().removeField("ts_day").commit();
        table.updateSchema().deleteColumn("ts").commit();
        table.refresh();
        // V1 retains a void partition field after its source column has been removed.
        Assertions.assertFalse(IcebergPartitionUtils.isValidRelatedTable(table));
    }

    @Test
    public void negatedPartialConjunctionMustNotPruneMatchingRows() {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.DateType.get()));
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "negated"), schema,
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
        org.apache.iceberg.TableOperations ops = ((org.apache.iceberg.HasTableOperations) table).operations();
        org.apache.iceberg.TableMetadata base = ops.current();
        ops.commit(base, org.apache.iceberg.TableMetadata.buildFrom(base).setCurrentSchema(new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone())), 2).build());
        table.refresh();
        ConnectorComparison ts = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ts", ConnectorType.of("TIMESTAMP_NS")),
                new ConnectorLiteral(ConnectorType.of("STRING"), "2024-01-02"));
        ConnectorComparison id = new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                new ConnectorLiteral(ConnectorType.of("INT"), 0));
        IcebergPredicateConverter converter = IcebergPredicateConverter.forTable(
                table, table.schema(), ZoneId.of("UTC"), IcebergPredicateConverter.Mode.SCAN);
        ConnectorAnd partial = new ConnectorAnd(java.util.Arrays.asList(ts, id));
        Assertions.assertEquals(1, converter.convert(partial).size());
        // id=1, ts=Jan 3 must survive NOT(ts=Jan 2 AND id>0); NOT(id>0) would lose it.
        Assertions.assertTrue(converter.convert(new ConnectorNot(partial)).isEmpty());
        Assertions.assertEquals(1, converter.convert(new ConnectorNot(id)).size());
    }

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
    public void predicatesPreserveNanosecondsInEveryMode() {
        Schema schema = new Schema(Types.NestedField.optional(1, "ts", Types.TimestampNanoType.withoutZone()));
        for (IcebergPredicateConverter.Mode mode : IcebergPredicateConverter.Mode.values()) {
            for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
                IcebergPredicateConverter converter = new IcebergPredicateConverter(schema, ZoneId.of(zone), mode);
                for (String text : new String[] {"2024-01-02 03:04:05.123456789",
                        "1969-12-31 23:59:59.999999999", "1677-09-21 00:12:43.145224192",
                        "2262-04-11 23:47:16.854775807"}) {
                    long expected = (long) IcebergPartitionUtils.parsePartitionValueFromString(
                            text, Types.TimestampNanoType.withoutZone(), ZoneId.of(zone));
                    for (ConnectorLiteral literal : new ConnectorLiteral[] {
                            new ConnectorLiteral(ConnectorType.of("STRING"), text),
                            new ConnectorLiteral(ConnectorType.of("TIMESTAMP_NS"),
                                    LocalDateTime.parse(text.replace(' ', 'T')))}) {
                        List<Expression> expressions = converter.convert(new ConnectorComparison(
                                ConnectorComparison.Operator.EQ,
                                new ConnectorColumnRef("ts", ConnectorType.of("TIMESTAMP_NS")), literal));
                        Assertions.assertEquals(1, expressions.size(), mode + " " + text);
                        Assertions.assertEquals(expected,
                                ((UnboundPredicate<?>) expressions.get(0)).literal().value());
                    }
                }
            }
        }
    }

    @Test
    public void offsetPartitionsUseSessionCivilTimeAndKeepNanoseconds() {
        String input = "2024-01-01 00:00:00.123456789+08:00";
        String[] zones = {"UTC", "Asia/Shanghai", "America/New_York"};
        String[] expected = {"2023-12-31 16:00:00.123456789", "2024-01-01 00:00:00.123456789",
                "2023-12-31 11:00:00.123456789"};
        for (int i = 0; i < zones.length; i++) {
            ZoneId zone = ZoneId.of(zones[i]);
            Assertions.assertEquals(IcebergPartitionUtils.parsePartitionValueFromString(
                    expected[i], Types.TimestampNanoType.withoutZone(), zone),
                    IcebergPartitionUtils.parsePartitionValueFromString(
                            input, Types.TimestampNanoType.withoutZone(), zone));
        }
    }

    @Test
    public void writeDefaultsPreserveNanoseconds() {
        for (long nanos : new long[] {Long.MIN_VALUE, -1, 0, 1, 123456789, Long.MAX_VALUE}) {
            String sql = IcebergWriteSchemaContext.toDorisSql(
                    Types.TimestampNanoType.withoutZone(), nanos, false, false);
            Assertions.assertTrue(sql.startsWith("'") && sql.endsWith("'"));
            String text = sql.substring(1, sql.length() - 1);
            Assertions.assertEquals(nanos, IcebergPartitionUtils.parsePartitionValueFromString(
                    text, Types.TimestampNanoType.withoutZone(), ZoneId.of("UTC")));
        }
    }

    @Test
    public void addColumnDefaultsAcceptNanosecondDateTimes() {
        String text = "2024-01-02 03:04:05.123456789";
        Assertions.assertEquals(1704164645123456789L, IcebergSchemaBuilder.parseDefaultLiteral(
                text, Types.TimestampNanoType.withoutZone()).value());
        Assertions.assertEquals(Long.MIN_VALUE, IcebergSchemaBuilder.parseDefaultLiteral(
                "1677-09-21 00:12:43.145224192", Types.TimestampNanoType.withoutZone()).value());
        Assertions.assertThrows(IllegalArgumentException.class, () -> IcebergSchemaBuilder.parseDefaultLiteral(
                "2262-04-11 23:47:16.854775808", Types.TimestampNanoType.withoutZone()));
    }

    @Test
    public void addColumnDefaultSurvivesSchemaCommitAndWritePlanning() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "defaults"),
                new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), Collections.singletonMap("format-version", "3"));
        String text = "2024-01-02 03:04:05.123456789";
        table.updateSchema().addColumn("ts", Types.TimestampNanoType.withoutZone(), null,
                IcebergSchemaBuilder.parseDefaultLiteral(text, Types.TimestampNanoType.withoutZone())).commit();
        Assertions.assertEquals(1704164645123456789L, table.schema().findField("ts").initialDefault());
        Assertions.assertEquals(1704164645123456789L, table.schema().findField("ts").writeDefault());
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.create(
                table, "defaults", Optional.empty(), false, false);
        Assertions.assertEquals("'" + text + "'", context.getColumns().get(1).getDefaultValueSql());
    }

    @Test
    public void staticIdentityPartitionAcceptsDateOnly() {
        for (String zone : new String[] {"UTC", "Asia/Shanghai", "America/New_York"}) {
            Object expected = IcebergPartitionUtils.parsePartitionValueFromString(
                    "2024-01-01 00:00:00", Types.TimestampNanoType.withoutZone(), ZoneId.of(zone));
            Assertions.assertEquals(expected, IcebergPartitionUtils.parsePartitionValueFromString(
                    "2024-01-01", Types.TimestampNanoType.withoutZone(), ZoneId.of(zone)));
        }
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

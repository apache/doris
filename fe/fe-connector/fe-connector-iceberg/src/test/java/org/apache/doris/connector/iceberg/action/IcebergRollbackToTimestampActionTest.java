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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.connector.iceberg.IcebergTimeUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

/**
 * Pins {@code rollback_to_timestamp}, including the connector-specific time-zone handling.
 *
 * <p><b>WHY this matters:</b> the timestamp argument is parsed in the SESSION time zone, the one piece of
 * legacy behaviour the connector cannot inherit from {@code ConnectContext}. The millisecond format
 * ({@code yyyy-MM-dd HH:mm:ss.SSS}, NOT the second format of {@code FOR TIME AS OF}) and the Doris alias map
 * (CST -> Asia/Shanghai) must both survive, or a {@code rollback_to_timestamp 'CST datetime'} pins the wrong
 * snapshot. {@link IcebergRollbackToTimestampAction#parseTimestampMillis} keeps the legacy millis-first,
 * {@code -1}-sentinel contract.</p>
 */
public class IcebergRollbackToTimestampActionTest {

    private static IcebergRollbackToTimestampAction action(String timestamp) {
        return new IcebergRollbackToTimestampAction(
                ImmutableMap.of("timestamp", timestamp), Collections.emptyList(), null);
    }

    // ─────────────────── parseTimestampMillis: TZ + format parity (deterministic) ───────────────────

    @Test
    public void parsesEpochMillisDirectly() {
        Assertions.assertEquals(1700000000000L,
                IcebergRollbackToTimestampAction.parseTimestampMillis("1700000000000", ZoneId.of("UTC")));
    }

    @Test
    public void parsesMillisDatetimeInGivenZoneNotUtc() {
        long shanghai = IcebergRollbackToTimestampAction.parseTimestampMillis(
                "2023-01-01 00:00:00.000", ZoneId.of("Asia/Shanghai"));
        long expected = LocalDateTime.parse("2023-01-01T00:00:00")
                .atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
        Assertions.assertEquals(expected, shanghai, "the datetime must be interpreted in the session zone");
        // The same wall-clock string in UTC is 8h later in epoch terms -> proves the zone is actually applied.
        Assertions.assertNotEquals(
                IcebergRollbackToTimestampAction.parseTimestampMillis("2023-01-01 00:00:00.000", ZoneId.of("UTC")),
                shanghai);
    }

    @Test
    public void cstAliasResolvesToShanghaiThroughSessionZone() {
        // The connector resolves the session time zone through the Doris alias map; CST is Asia/Shanghai
        // (NOT the US Central a bare ZoneId.of would reject). This is what the action feeds parseTimestampMillis.
        Assertions.assertEquals(ZoneId.of("Asia/Shanghai"),
                IcebergTimeUtils.resolveSessionZone(ActionTestTables.session("CST")));
    }

    @Test
    public void rejectsNegativeEpochMillis() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergRollbackToTimestampAction.parseTimestampMillis("-5", ZoneId.of("UTC")));
        Assertions.assertEquals("Timestamp must be non-negative: -5", e.getMessage());
    }

    @Test
    public void rejectsUnparseableTimestampWithLegacyMessage() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergRollbackToTimestampAction.parseTimestampMillis("not-a-time", ZoneId.of("UTC")));
        Assertions.assertEquals("Invalid timestamp format. Expected ISO datetime "
                + "(yyyy-MM-dd HH:mm:ss.SSS) or timestamp in milliseconds: not-a-time", e.getMessage());
    }

    // ─────────────────── argument validation (verbatim custom parser) ───────────────────

    @Test
    public void argumentValidationRejectsBadFormat() {
        IcebergRollbackToTimestampAction action = action("2023/01/01");
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, action::validate);
        Assertions.assertTrue(e.getMessage().contains("Invalid value for argument 'timestamp'"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("Invalid timestamp format"), e.getMessage());
    }

    // ─────────────────── full body against InMemoryCatalog ───────────────────

    @Test
    public void rollsBackToSnapshotCurrentAtTimestamp() throws InterruptedException {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long t1 = catalog.loadTable(id).currentSnapshot().timestampMillis();
        Thread.sleep(5); // ensure snap2 gets a strictly-later commit timestamp
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        Assertions.assertTrue(catalog.loadTable(id).currentSnapshot().timestampMillis() > t1);

        // Roll back to a time after snap1 but before snap2 (rollbackToTime needs a snapshot strictly older than
        // the target). The latest such snapshot is snap1. Result is (previous=snap2, current=snap1).
        IcebergRollbackToTimestampAction action = action(String.valueOf(t1 + 1));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0));
        Assertions.assertEquals(snap1, catalog.loadTable(id).currentSnapshot().snapshotId());
    }

    // ─────────────────── result schema + partition/WHERE rejection (T08 byte-parity) ───────────────────

    @Test
    public void resultSchemaIsTwoNotNullBigints() {
        List<ConnectorColumn> schema = action("1700000000000").getResultSchema();
        Assertions.assertEquals(2, schema.size());
        Assertions.assertEquals("previous_snapshot_id", schema.get(0).getName());
        Assertions.assertEquals("BIGINT", schema.get(0).getType().getTypeName());
        Assertions.assertFalse(schema.get(0).isNullable());
        Assertions.assertEquals("current_snapshot_id", schema.get(1).getName());
        Assertions.assertEquals("BIGINT", schema.get(1).getType().getTypeName());
        Assertions.assertFalse(schema.get(1).isNullable());
    }

    @Test
    public void rejectsPartitionSpec() {
        IcebergRollbackToTimestampAction a = new IcebergRollbackToTimestampAction(
                ImmutableMap.of("timestamp", "1700000000000"), ImmutableList.of("p1"), null);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'rollback_to_timestamp' does not support partition specification",
                e.getMessage());
    }

    @Test
    public void rejectsWhereCondition() {
        ConnectorPredicate where =
                new ConnectorPredicate(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE));
        IcebergRollbackToTimestampAction a = new IcebergRollbackToTimestampAction(
                ImmutableMap.of("timestamp", "1700000000000"), Collections.emptyList(), where);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'rollback_to_timestamp' does not support WHERE condition",
                e.getMessage());
    }
}

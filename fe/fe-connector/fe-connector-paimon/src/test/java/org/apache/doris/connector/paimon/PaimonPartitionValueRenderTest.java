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

package org.apache.doris.connector.paimon;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Unit tests for {@link PaimonScanPlanProvider#serializePartitionValue}, the FIX-NATIVE-PARTVAL
 * per-type partition-value renderer (byte-faithful port of legacy
 * {@code PaimonUtil.serializePartitionValue}).
 *
 * <p>For native ORC/Parquet reads BE materializes partition columns from this string (columnsFromPath);
 * a wrong string ⇒ wrong materialized rows. The pure static is the established testable seam (the
 * map wrapper {@code getPartitionInfoMap} needs a real {@code BinaryRow}+converter, heavy offline —
 * the same reason {@code shouldUseNativeReader} is tested as a pure static). Each test FAILS before
 * the fix (raw {@code Object.toString()}) and PASSES after, and encodes WHY (the BE contract), not
 * just WHAT. Offline, no fe-core, no Mockito — pure paimon {@code DataTypes}/{@code Timestamp}.
 */
public class PaimonPartitionValueRenderTest {

    @Test
    public void dateRendersAsIsoDateNotEpochDays() {
        int epochDays = (int) LocalDate.of(2024, 1, 1).toEpochDay();
        String rendered = PaimonScanPlanProvider.serializePartitionValue(
                DataTypes.DATE(), Integer.valueOf(epochDays), "UTC");

        // WHY: RowDataToObjectArrayConverter yields a boxed Integer epoch-days for DATE; a raw
        // toString() emits "19723" which BE parses as a garbage date -> data corruption. The ISO
        // render is the contract BE's columnsFromPath expects. MUTATION: raw toString() -> "19723" -> red.
        Assertions.assertEquals("2024-01-01", rendered);
    }

    @Test
    public void ltzCarriesExplicitUtcOffset() {
        // Paimon stores LTZ as the UTC instant; build the UTC wall clock 2024-01-01T01:02:03.
        Timestamp utcWallClock = Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 1, 2, 3));

        String shanghai = PaimonScanPlanProvider.serializePartitionValue(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), utcWallClock, "Asia/Shanghai");
        String utc = PaimonScanPlanProvider.serializePartitionValue(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), utcWallClock, "UTC");

        // The BE applies the session zone; transport must retain an unambiguous instant.
        Assertions.assertEquals("2024-01-01T01:02:03Z", shanghai);
        Assertions.assertEquals("2024-01-01T01:02:03Z", utc);
    }

    @Test
    public void ntzRendersIsoNoZoneShift() {
        Timestamp ts = Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 1, 2, 3));
        String rendered = PaimonScanPlanProvider.serializePartitionValue(
                DataTypes.TIMESTAMP(), ts, "Asia/Shanghai");

        // WHY: TIMESTAMP_WITHOUT_TIME_ZONE is a wall clock and must NOT be zone-shifted, regardless
        // of the session zone (the memory-note caveat: NTZ stays wall-clock). Guards against a future
        // "shift everything" regression. MUTATION: applying the session zone to NTZ -> "...T09:02:03" -> red.
        Assertions.assertEquals("2024-01-01T01:02:03", rendered);
    }

    @Test
    public void binaryPartitionsPreserveBytesAndNulls() {
        // Path values must decode into the same bytes as a Fluss log partition key.
        for (org.apache.paimon.types.DataType type : new org.apache.paimon.types.DataType[] {
                DataTypes.BINARY(4), DataTypes.VARBINARY(4)}) {
            Assertions.assertEquals("0x00017FFF", PaimonScanPlanProvider.serializePartitionValue(
                    type, new byte[] {0, 1, 127, (byte) 255}, "UTC"));
            Assertions.assertEquals("0x", PaimonScanPlanProvider.serializePartitionValue(type, new byte[0], "UTC"));
            Assertions.assertNull(PaimonScanPlanProvider.serializePartitionValue(type, null, "UTC"));
        }
    }

    @Test
    public void floatDoubleUseToStringRender() {
        // WHY: parity with legacy Float.toString / Double.toString. MUTATION: a different numeric
        // render -> red.
        Assertions.assertEquals("1.5",
                PaimonScanPlanProvider.serializePartitionValue(DataTypes.FLOAT(), 1.5f, "UTC"));
        Assertions.assertEquals("2.25",
                PaimonScanPlanProvider.serializePartitionValue(DataTypes.DOUBLE(), 2.25d, "UTC"));
    }

    @Test
    public void integerRendersViaToString() {
        // WHY: scalar types (the common partition case) go through value.toString(); pins the
        // base-case parity. MUTATION: mis-routing INTEGER to a typed branch -> red.
        Assertions.assertEquals("42",
                PaimonScanPlanProvider.serializePartitionValue(DataTypes.INT(), 42, "UTC"));
    }

    @Test
    public void nullValueRendersNull() {
        // WHY: every case null-guards (returns null), preserved from legacy; PaimonScanRange handles the
        // null entries downstream. MUTATION: NPE or "null" string -> red.
        Assertions.assertNull(
                PaimonScanPlanProvider.serializePartitionValue(DataTypes.INT(), null, "UTC"));
        Assertions.assertNull(
                PaimonScanPlanProvider.serializePartitionValue(DataTypes.DATE(), null, "UTC"));
        Assertions.assertNull(PaimonScanPlanProvider.serializePartitionValue(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), null, "Asia/Shanghai"));
    }
}

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

package org.apache.doris.connector.hive;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests that {@link HiveScanRange#populateRangeParams} emits {@code columns_from_path} from the
 * connector's partition values.
 *
 * <p>WHY: hive was the last connector still relying on fe-core's
 * {@code FilePartitionUtils.normalizeColumnsFromPath} to translate the Hive default-partition
 * directory sentinel ({@code __HIVE_DEFAULT_PARTITION__}) into a SQL NULL before sending it to BE.
 * That is a Hive-specific convention and must live in the Hive connector (iron rule: fe-core keeps no
 * source-specific string matching), so this connector now owns the sentinel-&gt;NULL mapping, mirroring
 * {@code IcebergScanRange}/{@code PaimonScanRange}. These tests pin: (a) real values pass through with
 * {@code is_null=false}; (b) the sentinel maps to {@code ""}+{@code is_null=true}; (c) a LITERAL
 * {@code "\N"} is a genuine value, NOT null (i.e. we use the narrow {@code .equals}, not
 * {@code ConnectorPartitionValues.normalize}); (d) an unpartitioned range emits nothing; (e) an ACID
 * range emits BOTH the transactional params and the partition values. The emitted keys are the
 * partition column names in {@code path_partition_keys} order, keeping the bytes identical to the
 * legacy engine-side path.</p>
 */
public class HiveScanRangePartitionValuesTest {

    private static Map<String, String> orderedPartitionValues(String... keyThenValue) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < keyThenValue.length; i += 2) {
            map.put(keyThenValue[i], keyThenValue[i + 1]);
        }
        return map;
    }

    private static TFileRangeDesc populate(HiveScanRange range) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);
        return rangeDesc;
    }

    @Test
    public void testRealPartitionValuesEmittedNotNull() {
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/dt=2024-01-01/city=beijing/000000_0")
                .partitionValues(orderedPartitionValues("dt", "2024-01-01", "city", "beijing"))
                .build();

        TFileRangeDesc rangeDesc = populate(range);

        Assertions.assertEquals(Arrays.asList("dt", "city"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Arrays.asList("2024-01-01", "beijing"), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Arrays.asList(false, false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testDefaultPartitionSentinelMapsToSqlNull() {
        // The HMS default-partition directory sentinel is a genuine SQL NULL: value -> "" + is_null=true.
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/dt=2024-01-01/city=__HIVE_DEFAULT_PARTITION__/000000_0")
                .partitionValues(orderedPartitionValues(
                        "dt", "2024-01-01", "city", "__HIVE_DEFAULT_PARTITION__"))
                .build();

        TFileRangeDesc rangeDesc = populate(range);

        Assertions.assertEquals(Arrays.asList("dt", "city"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Arrays.asList("2024-01-01", ""), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testLiteralBackslashNIsNotNull() {
        // A literal "\N" partition value is NOT the Hive default-partition sentinel and must survive as a
        // real value (is_null=false). This is why the connector uses the narrow HIVE_DEFAULT_PARTITION.equals
        // and NOT ConnectorPartitionValues.normalize (which would coerce "\N" to SQL NULL).
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/city=%5CN/000000_0")
                .partitionValues(orderedPartitionValues("city", "\\N"))
                .build();

        TFileRangeDesc rangeDesc = populate(range);

        Assertions.assertEquals(Collections.singletonList("city"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("\\N"), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testUnpartitionedEmitsNoColumnsFromPath() {
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/000000_0")
                .fileFormat("parquet")
                .build();

        TFileRangeDesc rangeDesc = populate(range);

        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys());
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathIsNull());
    }

    @Test
    public void testAcidTableEmitsBothTransactionalParamsAndPartitionValues() {
        // ACID (transactional_hive) ranges must still carry columns-from-path in addition to the
        // transactional delete-delta params; the partition-value rebuild runs regardless of ACID.
        HiveScanRange range = HiveScanRange.builder()
                .path("/tbl/dt=2024-01-01/delta_0000005_0000005/bucket_00000")
                .partitionValues(orderedPartitionValues("dt", "2024-01-01"))
                .acidInfo("/tbl/dt=2024-01-01", Arrays.asList(
                        "/tbl/dt=2024-01-01/delete_delta_0000003_0000003|bucket_00000"))
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertTrue(formatDesc.isSetTransactionalHiveParams(),
                "ACID range must still emit transactional params");
        Assertions.assertEquals(Collections.singletonList("dt"), rangeDesc.getColumnsFromPathKeys());
        Assertions.assertEquals(Collections.singletonList("2024-01-01"), rangeDesc.getColumnsFromPath());
        Assertions.assertEquals(Collections.singletonList(false), rangeDesc.getColumnsFromPathIsNull());
    }
}

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

package org.apache.doris.connector.hudi;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pins the {@code columns_from_path} / {@code columns_from_path_is_null} bytes
 * {@link HudiScanRange#populateRangeParams} sends to BE for every shape a hudi partition directory name can
 * take. (Distinct from {@code HudiPartitionValuesTest}, which covers the earlier step — parsing a partition
 * PATH into the name/value map this class consumes.)
 *
 * <p>WHY these three inputs all mean SQL NULL: a hudi partition value is a directory name. Hive-style writers
 * spell a NULL partition {@code __HIVE_DEFAULT_PARTITION__}, and a literal {@code \N} directory is the older
 * text-table spelling of the same thing. That equivalence holds ONLY for directory-name partitioning, which is
 * why it lives in the hudi connector and not in the neutral module: hive narrows it (no {@code \N} arm, because
 * a hive column may legitimately contain the two characters as DATA) and paimon rejects it outright (its
 * partition values are typed, so {@code \N} is ordinary string data). See the WHY comment on the block under
 * test.</p>
 *
 * <p>WHY this must not drift: {@code docker/thirdparties/.../hudi/11_create_mtmv_tables.sql} creates a real
 * fixture table partitioned on {@code region='__HIVE_DEFAULT_PARTITION__'}, and the rendered value plus its
 * null flag are what BE fills the partition column from
 * ({@code partition_column_filler.h#fill_partition_column_from_path_value}).</p>
 *
 * <p>MUTATION NOTES (each arm is separately killable):</p>
 * <ul>
 *   <li>render the raw value instead of the NULL spelling -> the sentinel row AND the java-null row of
 *       {@code columns_from_path} go red;</li>
 *   <li>drop the literal-{@code \N} arm of the null test -> {@code columns_from_path} stays GREEN (the value is
 *       already {@code \N} either way); only {@code columns_from_path_is_null} catches it. That column is the
 *       sole detector for that mutation — do not reduce this test to a value-only assertion;</li>
 *   <li>treat the empty string as NULL (e.g. an {@code isEmpty()} rewrite) -> the empty row goes red.</li>
 * </ul>
 */
public class HudiScanRangePartitionValuesTest {

    @Test
    public void pathPartitionValuesRenderEveryNullSpelling() {
        // One range carrying all five shapes at once, so the assertions also pin keys <-> values <-> flags
        // alignment (BE zips the three lists positionally). LinkedHashMap: the render walks entrySet(), so
        // iteration order IS the emitted order.
        Map<String, String> partValues = new LinkedHashMap<>();
        partValues.put("d_plain", "2024-01-01");
        partValues.put("d_sentinel", "__HIVE_DEFAULT_PARTITION__");
        // Reachable in production: parsePartitionValues strips the "col=" prefix, so a "dt=" path fragment
        // yields "". It is a real (empty) value, NOT null.
        partValues.put("d_empty", "");
        partValues.put("d_literal_null", "\\N");
        // Defensive arm only: parsePartitionValues always puts an unescapePathName() result, never null. Kept
        // because the render still branches on it, and a HashMap-built range (as here) can carry one.
        partValues.put("d_java_null", null);

        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/base.parquet")
                .fileFormat("parquet")
                .fileSize(456L)
                .partitionValues(partValues)
                .build();

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);

        Assertions.assertEquals(
                Arrays.asList("d_plain", "d_sentinel", "d_empty", "d_literal_null", "d_java_null"),
                rangeDesc.getColumnsFromPathKeys(),
                "partition column names go to BE verbatim, in map order");
        Assertions.assertEquals(
                Arrays.asList("2024-01-01", "\\N", "", "\\N", "\\N"),
                rangeDesc.getColumnsFromPath(),
                "hudi renders a NULL partition as \\N (hive/paimon/iceberg render \"\" — do not unify here)");
        Assertions.assertEquals(
                Arrays.asList(false, true, false, true, true),
                rangeDesc.getColumnsFromPathIsNull(),
                "the flag, not the string, is what makes BE emit SQL NULL");
    }

    @Test
    public void noPartitionValuesLeavesColumnsFromPathUnset() {
        // An unpartitioned slice must not stamp empty lists: on a fresh range desc the three fields stay unset.
        // NOTE this is an assertion about THIS method only — unlike hive and iceberg, hudi does not unset what
        // the engine pre-filled, so in production an empty map leaves any pre-filled values in place.
        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/base.parquet")
                .fileFormat("parquet")
                .fileSize(456L)
                .partitionValues(Collections.emptyMap())
                .build();

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);

        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathKeys(), "no keys for an unpartitioned slice");
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPath(), "no values for an unpartitioned slice");
        Assertions.assertFalse(rangeDesc.isSetColumnsFromPathIsNull(), "no flags for an unpartitioned slice");
    }
}

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

import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.THudiFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests {@link HudiScanRange#populateRangeParams}.
 *
 * <p>WHY: column_names/column_types/delta_logs are thrift {@code list<string>};
 * BE ({@code hudi_jni_reader.cpp}) joins them with distinct delimiters
 * (names ',', types '#', delta logs ','). The FE must pass each per-column type
 * as a single list element. The previous code joined them with ',' and split
 * back by ',', which shattered comma-bearing Hive type strings
 * ({@code decimal(10,2)}, {@code struct<...>}) and misaligned names/types.
 * These tests pin that the typed lists survive intact and aligned.</p>
 */
public class HudiScanRangeTest {

    @Test
    public void testJniListsSurviveIntactAndAligned() {
        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/file")
                .fileFormat("jni")
                .instantTime("20240101000000000")
                .serde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .inputFormat("org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat")
                .basePath("s3://bucket/t")
                .dataFilePath("s3://bucket/t/base.parquet")
                .dataFileLength(123L)
                .deltaLogs(Arrays.asList("s3://bucket/t/.f.log.1_0", "s3://bucket/t/.f.log.2_0"))
                .columnNames(Arrays.asList("x", "y", "z"))
                .columnTypes(Arrays.asList("int", "decimal(10,2)", "struct<a:int,b:string>"))
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        THudiFileDesc fileDesc = formatDesc.getHudiParams();

        // A log-bearing MOR slice must set FORMAT_JNI explicitly (not rely on the node default).
        Assertions.assertEquals(TFileFormatType.FORMAT_JNI, rangeDesc.getFormatType());

        // Types must NOT be shattered: 3 columns -> 3 type strings (old bug
        // produced 5: "decimal(10","2)","struct<a:int","b:string>").
        Assertions.assertEquals(Arrays.asList("int", "decimal(10,2)", "struct<a:int,b:string>"),
                fileDesc.getColumnTypes());
        Assertions.assertEquals(Arrays.asList("x", "y", "z"), fileDesc.getColumnNames());
        Assertions.assertEquals(Arrays.asList("s3://bucket/t/.f.log.1_0", "s3://bucket/t/.f.log.2_0"),
                fileDesc.getDeltaLogs());

        // names <-> types alignment (the JNI scanner zips them positionally).
        Assertions.assertEquals(fileDesc.getColumnNames().size(), fileDesc.getColumnTypes().size());
    }

    @Test
    public void testNoDeltaLogsDowngradesToNativeParquet() {
        // MOR file slice with no delta logs -> native parquet reader; no JNI lists set.
        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/base.parquet")
                .fileFormat("jni")
                .dataFilePath("s3://bucket/t/base.parquet")
                .dataFileLength(456L)
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType());
        Assertions.assertFalse(formatDesc.getHudiParams().isSetColumnTypes());
        Assertions.assertFalse(formatDesc.getHudiParams().isSetColumnNames());
    }

    @Test
    public void nativeParquetSliceExplicitlySetsFormatType() {
        // A native slice as collectMorSplits/collectCowSplits actually build it: fileFormat="parquet" (NOT
        // "jni"), no JNI metadata. populateRangeParams MUST set FORMAT_PARQUET explicitly — relying on the
        // node-level default (jni for a MOR table) shipped an empty THudiFileDesc under FORMAT_JNI to BE.
        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/base.parquet")
                .fileFormat("parquet")
                .fileSize(456L)
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType(),
                "a native parquet slice must set FORMAT_PARQUET, not inherit the node default");
        Assertions.assertFalse(formatDesc.getHudiParams().isSetColumnNames(), "native slice sets no JNI fields");
    }

    @Test
    public void nativeOrcSliceExplicitlySetsFormatType() {
        // A COW ORC table's node default is parquet; without an explicit per-range set BE would read the ORC
        // base file as parquet.
        HudiScanRange range = new HudiScanRange.Builder()
                .path("s3://bucket/t/base.orc")
                .fileFormat("orc")
                .fileSize(456L)
                .build();

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(TFileFormatType.FORMAT_ORC, rangeDesc.getFormatType(),
                "a native orc slice must set FORMAT_ORC, not inherit the parquet node default");
    }
}

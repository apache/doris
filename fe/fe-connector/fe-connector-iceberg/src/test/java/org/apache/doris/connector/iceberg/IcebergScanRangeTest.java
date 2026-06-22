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

import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Skeleton tests for {@link IcebergScanRange} (P6.2-T01), mirroring the paimon connector's
 * {@code PaimonScanRange} carrier. Only the minimal {@code FILE_SCAN} file fields (path/start/length/
 * size/format) exist this task; the per-range delete-file / JNI-split / schema-id / partition / COUNT
 * carriers and {@code populateRangeParams} land in P6.2-T02..T09. These pin the carrier the builder
 * produces today so a later task that breaks the FILE_SCAN contract fails loudly.
 */
public class IcebergScanRangeTest {

    @Test
    public void builderProducesFileScanRangeWithFileFields() {
        IcebergScanRange range = new IcebergScanRange.Builder()
                .path("s3://bucket/db/t/data/f.parquet")
                .start(128L)
                .length(4096L)
                .fileSize(8192L)
                .fileFormat("parquet")
                .build();

        // WHY: iceberg is a file-based connector, so the engine must build a TFileScanRange off this range.
        // MUTATION: returning JDBC_SCAN / CUSTOM -> wrong thrift scan-range variant -> red.
        Assertions.assertEquals(ConnectorScanRangeType.FILE_SCAN, range.getRangeType());
        Assertions.assertEquals(Optional.of("s3://bucket/db/t/data/f.parquet"), range.getPath());
        Assertions.assertEquals(128L, range.getStart());
        Assertions.assertEquals(4096L, range.getLength());
        Assertions.assertEquals(8192L, range.getFileSize());
        Assertions.assertEquals("parquet", range.getFileFormat());
        // WHY: BE selects its iceberg reader off TTableFormatFileDesc.table_format_type, whose value for
        // iceberg is "iceberg" (TableFormatType.ICEBERG). MUTATION: "paimon" / "plugin_driven" -> BE routes
        // the split to the wrong reader -> red.
        Assertions.assertEquals("iceberg", range.getTableFormatType());
    }

    @Test
    public void builderDefaultsMatchFileScanContract() {
        // A range built with only a path keeps the ConnectorScanRange contract defaults: start=0,
        // length=-1 (whole file), fileSize=-1 (unknown), fileFormat="" (not-yet-known, NOT "jni").
        // Pins that the skeleton does not invent values.
        IcebergScanRange range = new IcebergScanRange.Builder().path("/tmp/x").build();
        Assertions.assertEquals(0L, range.getStart());
        Assertions.assertEquals(-1L, range.getLength());
        Assertions.assertEquals(-1L, range.getFileSize());
        Assertions.assertEquals("", range.getFileFormat());
        // No connector-specific per-range properties exist yet (T03 introduces them); must be non-null.
        Assertions.assertNotNull(range.getProperties());
        Assertions.assertTrue(range.getProperties().isEmpty());
    }
}

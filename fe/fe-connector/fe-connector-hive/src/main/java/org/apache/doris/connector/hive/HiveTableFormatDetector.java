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

import org.apache.doris.connector.hms.HmsTableInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Detects the actual table format of a table registered in HMS.
 *
 * <p>HMS can store Hive, Hudi, and Iceberg tables under the same metastore.
 * This class mirrors the detection logic from fe-core's
 * {@code HMSExternalTable.supportedIcebergTable/supportedHoodieTable/supportedHiveTable}
 * using only SPI types.</p>
 */
public final class HiveTableFormatDetector {

    private static final Set<String> SUPPORTED_HIVE_INPUT_FORMATS;
    private static final Set<String> SUPPORTED_HUDI_INPUT_FORMATS;

    static {
        Set<String> hiveFormats = new HashSet<>();
        hiveFormats.add("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        hiveFormats.add("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        hiveFormats.add("org.apache.hadoop.mapred.TextInputFormat");
        // Some Hudi tables use HoodieParquetInputFormatBase as input format
        // but cannot be treated as Hudi — read parquet files directly as Hive.
        hiveFormats.add("org.apache.hudi.hadoop.HoodieParquetInputFormatBase");
        SUPPORTED_HIVE_INPUT_FORMATS = Collections.unmodifiableSet(hiveFormats);

        SUPPORTED_HUDI_INPUT_FORMATS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                "org.apache.hudi.hadoop.HoodieParquetInputFormat",
                "com.uber.hoodie.hadoop.HoodieInputFormat",
                "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat",
                "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat"
        )));
    }

    private HiveTableFormatDetector() {
    }

    /**
     * Detect the table format from HMS table metadata.
     *
     * <p>Detection order (matches fe-core):
     * <ol>
     *   <li>Iceberg: parameters contain {@code table_type=ICEBERG}</li>
     *   <li>Hudi: input format in SUPPORTED_HUDI_INPUT_FORMATS or
     *       {@code flink.connector=hudi}</li>
     *   <li>Hive: input format in SUPPORTED_HIVE_INPUT_FORMATS</li>
     *   <li>UNKNOWN: none of the above</li>
     * </ol>
     *
     * @param tableInfo HMS table metadata
     * @return detected table type
     */
    public static HiveTableType detect(HmsTableInfo tableInfo) {
        Map<String, String> params = tableInfo.getParameters();

        // 1. Iceberg detection
        if (params != null && "ICEBERG".equalsIgnoreCase(params.get("table_type"))) {
            return HiveTableType.ICEBERG;
        }

        // 2. Hudi detection
        String inputFormat = tableInfo.getInputFormat();
        if (params != null && "hudi".equalsIgnoreCase(params.get("flink.connector"))) {
            return HiveTableType.HUDI;
        }
        if (inputFormat != null && SUPPORTED_HUDI_INPUT_FORMATS.contains(inputFormat)) {
            return HiveTableType.HUDI;
        }

        // 3. Hive detection
        if (inputFormat != null && SUPPORTED_HIVE_INPUT_FORMATS.contains(inputFormat)) {
            return HiveTableType.HIVE;
        }

        return HiveTableType.UNKNOWN;
    }

    /**
     * Whether the given input format is a known Hive-native format.
     */
    public static boolean isHiveFormat(String inputFormat) {
        return inputFormat != null && SUPPORTED_HIVE_INPUT_FORMATS.contains(inputFormat);
    }
}

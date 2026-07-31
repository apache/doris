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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveTableFormatDetector#detect} format classification and the fail-loud + view short-circuit
 * that {@link HiveConnectorMetadata#getTableHandle} layers on top (HMS cutover §4.2).
 *
 * <p>WHY:</p>
 * <ul>
 *   <li><b>LZO parity.</b> Legacy {@code HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS} includes three
 *       LZO-text InputFormats; omitting them would make an LZO-text table fail the (now fail-loud) format
 *       check even though legacy read it as hive text.</li>
 *   <li><b>Fail-loud parity.</b> Legacy {@code supportedHiveTable()} threw on a null/unrecognized input
 *       format; the old connector silently returned UNKNOWN, which would let an unreadable table degrade
 *       instead of surfacing the error.</li>
 *   <li><b>View short-circuit.</b> A view has no data files (null input format) but is valid; legacy
 *       returned true for it before the format check. The fail-loud guard must skip views or every hive
 *       view would be rejected at handle resolution.</li>
 * </ul>
 */
public class HiveTableFormatDetectionTest {

    private static final String PARQUET = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String ORC = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private static final String TEXT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String HUDI = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

    // ===== detect(): pure format classification =====

    @Test
    public void lzoTextFormatsClassifyAsHive() {
        // All three LZO-text InputFormats legacy supported (com.hadoop.compression.lzo / mapreduce / mapred).
        Assertions.assertEquals(HiveTableType.HIVE,
                HiveTableFormatDetector.detect(hiveTable("com.hadoop.compression.lzo.LzoTextInputFormat")));
        Assertions.assertEquals(HiveTableType.HIVE,
                HiveTableFormatDetector.detect(hiveTable("com.hadoop.mapreduce.LzoTextInputFormat")));
        Assertions.assertEquals(HiveTableType.HIVE,
                HiveTableFormatDetector.detect(hiveTable("com.hadoop.mapred.DeprecatedLzoTextInputFormat")));
    }

    @Test
    public void standardFormatsClassifyCorrectly() {
        Assertions.assertEquals(HiveTableType.HIVE, HiveTableFormatDetector.detect(hiveTable(PARQUET)));
        Assertions.assertEquals(HiveTableType.HIVE, HiveTableFormatDetector.detect(hiveTable(ORC)));
        Assertions.assertEquals(HiveTableType.HIVE, HiveTableFormatDetector.detect(hiveTable(TEXT)));
        Assertions.assertEquals(HiveTableType.HUDI, HiveTableFormatDetector.detect(hiveTable(HUDI)));
        Assertions.assertEquals(HiveTableType.ICEBERG, HiveTableFormatDetector.detect(icebergTable()));
    }

    @Test
    public void unrecognizedOrNullFormatIsUnknown() {
        Assertions.assertEquals(HiveTableType.UNKNOWN,
                HiveTableFormatDetector.detect(hiveTable("com.example.MyCustomInputFormat")));
        Assertions.assertEquals(HiveTableType.UNKNOWN, HiveTableFormatDetector.detect(hiveTable(null)));
        // detect() is format-only: a view (no data-file format) also classifies UNKNOWN here; the
        // short-circuit that keeps it from being rejected lives in getTableHandle.
        Assertions.assertEquals(HiveTableType.UNKNOWN, HiveTableFormatDetector.detect(viewTable()));
    }

    // ===== getTableHandle(): fail-loud + view short-circuit =====

    @Test
    public void unsupportedFormatFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> handleFor(hiveTable("com.example.MyCustomInputFormat")));
        Assertions.assertTrue(ex.getMessage().contains("Unsupported hive input format"), ex.getMessage());
    }

    @Test
    public void nullFormatNonViewFailsLoud() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> handleFor(hiveTable(null)));
        Assertions.assertTrue(ex.getMessage().contains("input format is null"), ex.getMessage());
    }

    @Test
    public void viewIsNotRejected() {
        // A view has a null input format but must NOT fail loud; its handle keeps the UNKNOWN type (a view is
        // served by the view SPI, never scanned).
        Optional<ConnectorTableHandle> handle = handleFor(viewTable());
        Assertions.assertTrue(handle.isPresent(), "a hive view must resolve a handle, not be rejected");
        Assertions.assertEquals(HiveTableType.UNKNOWN, ((HiveTableHandle) handle.get()).getTableType());
    }

    @Test
    public void supportedFormatsResolveHandle() {
        Assertions.assertEquals(HiveTableType.HIVE,
                ((HiveTableHandle) handleFor(hiveTable(PARQUET)).get()).getTableType());
        // An LZO-text table now resolves instead of failing loud.
        Assertions.assertEquals(HiveTableType.HIVE,
                ((HiveTableHandle) handleFor(hiveTable("com.hadoop.compression.lzo.LzoTextInputFormat"))
                        .get()).getTableType());
    }

    // ===== helpers =====

    private Optional<ConnectorTableHandle> handleFor(HmsTableInfo tableInfo) {
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new FakeHmsClient(tableInfo), Collections.emptyMap(), new FakeConnectorContext());
        return metadata.getTableHandle(null, "db", "t");
    }

    private static HmsTableInfo hiveTable(String inputFormat) {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("MANAGED_TABLE")
                .inputFormat(inputFormat)
                .build();
    }

    private static HmsTableInfo icebergTable() {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("EXTERNAL_TABLE")
                .parameters(Collections.singletonMap("table_type", "ICEBERG"))
                .build();
    }

    private static HmsTableInfo viewTable() {
        return HmsTableInfo.builder()
                .dbName("db").tableName("t").tableType("VIRTUAL_VIEW")
                .viewOriginalText("SELECT 1")
                .build();
    }

    /** Minimal {@link HmsClient} double serving one prebuilt table; the rest fail loud. */
    private static final class FakeHmsClient implements HmsClient {
        private final HmsTableInfo table;

        FakeHmsClient(HmsTableInfo table) {
            this.table = table;
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return true;
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            return table;
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}

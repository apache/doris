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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/** Dependency-free microbenchmark for weighted Iceberg admission and per-statement metadata isolation. */
public final class IcebergTableCacheBenchmark {
    private static final int WARMUP_WINDOWS = 5;
    private static final int MEASURE_WINDOWS = 15;
    private static volatile long blackhole;

    private IcebergTableCacheBenchmark() {
    }

    public static void main(String[] args) {
        Table table = tableFixture();
        TableMetadata metadata = ((BaseTable) table).operations().current();
        String json = TableMetadataParser.toJson(metadata);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(
                    owner, CacheSpec.ofWeight(true, 100L, 1000L, 100L * 1024L * 1024L),
                    ignored -> () -> { }, new IcebergCatalogResourceTracker());
            try (IcebergTableCache.TableLease lease = cache.borrow(
                    TableIdentifier.of("db", "table"), () -> table)) {
                Result result = measure(() -> {
                    Table statementTable = lease.snapshotReadTable();
                    return ((BaseTable) statementTable).operations().current().properties().size();
                }, 100);
                System.out.printf(
                        "iceberg_metadata_json_chars=%d properties=%d statement_copy_ns_op=%d operations=%d%n",
                        json.length(), metadata.properties().size(), result.medianNanos, result.operations);
            }
        }
    }

    private static Table tableFixture() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 0; i < 1_000; i++) {
            properties.put("property-" + i, "value-" + i + "-" + "x".repeat(48));
        }
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:///tmp/weighted-table", properties);
        metadata = TableMetadata.buildFrom(metadata)
                .withMetadataLocation("file:///tmp/weighted-table/metadata/v1.metadata.json")
                .discardChanges()
                .build();
        return new BaseTable(new StaticTableOperations(metadata), "weighted");
    }

    private static Result measure(LongOperation operation, int operationsPerWindow) {
        for (int i = 0; i < WARMUP_WINDOWS; i++) {
            runWindow(operation, operationsPerWindow);
        }
        long[] nanosPerOperation = new long[MEASURE_WINDOWS];
        for (int i = 0; i < MEASURE_WINDOWS; i++) {
            long start = System.nanoTime();
            runWindow(operation, operationsPerWindow);
            nanosPerOperation[i] = (System.nanoTime() - start) / operationsPerWindow;
        }
        Arrays.sort(nanosPerOperation);
        return new Result(nanosPerOperation[MEASURE_WINDOWS / 2],
                (long) operationsPerWindow * MEASURE_WINDOWS);
    }

    private static void runWindow(LongOperation operation, int operations) {
        long value = 0L;
        for (int i = 0; i < operations; i++) {
            value ^= operation.run();
        }
        blackhole = value;
    }

    @FunctionalInterface
    private interface LongOperation {
        long run();
    }

    private static final class Result {
        private final long medianNanos;
        private final long operations;

        private Result(long medianNanos, long operations) {
            this.medianNanos = medianNanos;
            this.operations = operations;
        }
    }

    private static final class StaticTableOperations implements TableOperations {
        private final TableMetadata metadata;

        private StaticTableOperations(TableMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io() {
            return null;
        }

        @Override
        public EncryptionManager encryption() {
            return null;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return "file:///tmp/weighted-table/metadata/" + fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            return null;
        }
    }
}

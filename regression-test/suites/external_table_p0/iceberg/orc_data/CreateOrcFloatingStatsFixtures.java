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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.types.Types;

/** Standard SDK fixtures; neither data streams nor ORC statistics are edited. */
public final class CreateOrcFloatingStatsFixtures {
    private CreateOrcFloatingStatsFixtures() {
    }

    private static void sameValues(Map<String, Double[][]> cases, String name, Double... values) {
        Double[][] rows = new Double[values.length][2];
        for (int row = 0; row < values.length; ++row) {
            rows[row][0] = values[row];
            rows[row][1] = values[row];
        }
        cases.put(name, rows);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>();
        if (args.length == 2) {
            properties.put("type", "hadoop");
            properties.put("warehouse", args[1]);
        } else {
            properties.put("type", "rest");
            properties.put("uri", "http://rest:8181");
            properties.put("warehouse", "s3://warehouse/wh/");
            properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            properties.put("s3.endpoint", "http://minio:9000");
            properties.put("s3.path-style-access", "true");
            properties.put("s3.region", "us-east-1");
        }
        Catalog catalog = CatalogUtil.buildIcebergCatalog("orc_stats", properties, new Configuration());
        try (AutoCloseable ignored = (AutoCloseable) catalog) {
            Namespace namespace = Namespace.of(args[0]);
            SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
            if (!namespaces.namespaceExists(namespace)) {
                namespaces.createNamespace(namespace);
            }
            Map<String, Double[][]> cases = new LinkedHashMap<>();
            sameValues(cases, "nan_middle", 1.0, Double.NaN, 2.0);
            sameValues(cases, "nan_last", 1.0, 2.0, Double.NaN);
            sameValues(cases, "nan_first", Double.NaN, 1.0, 2.0);
            sameValues(cases, "all_nan", Double.NaN, Double.NaN);
            sameValues(cases, "nullable_nan", null, 1.0, Double.NaN, 2.0);
            sameValues(cases, "finite", null, -2.0, 3.0, 1.0);
            sameValues(cases, "all_null", null, null);
            sameValues(cases, "positive_inf", 1.0, Double.POSITIVE_INFINITY);
            sameValues(cases, "negative_inf", Double.NEGATIVE_INFINITY, 2.0);
            sameValues(cases, "opposite_inf", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
            cases.put("double_overflow", new Double[][] {{1.0, Double.MAX_VALUE}, {2.0, Double.MAX_VALUE}});
            cases.put("float_nan_only", new Double[][] {{1.0, 1.0}, {Double.NaN, 2.0}, {2.0, 3.0}});
            cases.put("double_nan_only", new Double[][] {{1.0, 1.0}, {2.0, Double.NaN}, {3.0, 2.0}});
            sameValues(cases, "empty");
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "f", Types.FloatType.get()),
                    Types.NestedField.optional(3, "d", Types.DoubleType.get()));
            for (Map.Entry<String, Double[][]> entry : cases.entrySet()) {
                TableIdentifier identifier = TableIdentifier.of(namespace, entry.getKey());
                catalog.dropTable(identifier, true);
                Table table = catalog.createTable(identifier, schema, PartitionSpec.unpartitioned(),
                        Map.of("format-version", "3", "write.format.default", "orc"));
                OutputFile file = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".orc");
                FileAppender<Record> appender = ORC.write(file).schema(schema)
                        .createWriterFunc(GenericOrcWriter::buildWriter).build();
                Double[][] rows = entry.getValue();
                try (FileAppender<Record> writer = appender) {
                    for (int id = 0; id < rows.length; ++id) {
                        GenericRecord record = GenericRecord.create(schema);
                        record.setField("id", id);
                        record.setField("f", rows[id][0] == null ? null : rows[id][0].floatValue());
                        record.setField("d", rows[id][1]);
                        writer.add(record);
                    }
                }
                // Column metrics are optional. Omitting them makes SQL reach physical ORC statistics.
                table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath(file.location())
                        .withFormat(FileFormat.ORC).withRecordCount(rows.length)
                        .withFileSizeInBytes(appender.length()).build()).commit();
                int seen = 0;
                try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                    for (Record record : records) {
                        Float expectedFloat = rows[seen][0] == null ? null : rows[seen][0].floatValue();
                        if (!Objects.equals(record.getField("id"), seen)
                                || !Objects.equals(record.getField("f"), expectedFloat)
                                || !Objects.equals(record.getField("d"), rows[seen][1])) {
                            throw new IllegalStateException("SDK row mismatch for " + entry.getKey() + " at " + seen);
                        }
                        ++seen;
                    }
                }
                if (seen != rows.length) {
                    throw new IllegalStateException("SDK row count mismatch for " + entry.getKey());
                }
                System.out.println("ORC_STATS_FIXTURE " + entry.getKey() + " " + seen);
            }
        }
    }
}

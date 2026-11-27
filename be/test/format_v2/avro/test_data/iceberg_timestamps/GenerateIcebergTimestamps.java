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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.GenericAvroReader;
import org.apache.iceberg.avro.GenericAvroWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Generates OCFs through the pinned Iceberg schema converter, appender and datum writer. */
public final class GenerateIcebergTimestamps {
    private static final String ROOT = "iceberg_timestamp_fixture";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long[] MICROS = {0L, 1L, -1L, 123456L, -123456L,
            1730611800123456L, 1730615400654321L};
    private static final long[] NANOS = {0L, 1L, -1L, 123456789L, -123456789L,
            1730611800123456789L, 1730615400654321987L};
    // -1 is a null struct; -2 is a present struct with a null child.
    private static final int[] NESTED = {0, -1, -2, 4, 3, 6, 5, -1};
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    private enum Kind {
        CIVIL_MICROS(false, false), INSTANT_MICROS(false, true),
        CIVIL_NANOS(true, false), INSTANT_NANOS(true, true);
        private final boolean nanos;
        private final boolean instant;
        Kind(boolean nanos, boolean instant) { this.nanos = nanos; this.instant = instant; }
        Type type() {
            return nanos
                    ? (instant ? Types.TimestampNanoType.withZone() : Types.TimestampNanoType.withoutZone())
                    : (instant ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone());
        }
        String logical() { return nanos ? "timestamp-nanos" : "timestamp-micros"; }
        long[] encodedValues() { return nanos ? NANOS : MICROS; }
        String file() { return "iceberg_" + name().toLowerCase(java.util.Locale.ROOT) + ".avro"; }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static Schema nonNull(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) return schema;
        List<Schema> choices = schema.getTypes().stream()
                .filter(value -> value.getType() != Schema.Type.NULL).toList();
        require(choices.size() == 1, "Expected one non-null branch");
        return choices.get(0);
    }

    private static void assertClock(Schema value, Kind kind) {
        Schema clock = nonNull(value);
        require(clock.getType() == Schema.Type.LONG, "Timestamp must use physical LONG");
        require(clock.getLogicalType() != null && kind.logical().equals(clock.getLogicalType().getName()),
                "Unexpected producer logical annotation");
        require(Boolean.valueOf(kind.instant).equals(clock.getObjectProp("adjust-to-utc")),
                "Unexpected producer adjust-to-utc value");
    }

    private static String sha256(byte[] value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    private static Map<String, Object> origin(Class<?> type, String expectedJar) throws Exception {
        Path jar = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        require(expectedJar.equals(jar.getFileName().toString()), "Unexpected class source for " + type.getName());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("class", type.getName());
        result.put("jar", jar.getFileName().toString());
        result.put("sha256", sha256(Files.readAllBytes(jar)));
        return result;
    }

    private static String civil(long value, Kind kind) {
        long unit = kind.nanos ? 1000000000L : 1000000L;
        int fraction = Math.toIntExact(Math.floorMod(value, unit) * (kind.nanos ? 1L : 1000L));
        // Negative subsecond values require floor arithmetic, not truncation toward zero.
        LocalDateTime local = LocalDateTime.ofEpochSecond(Math.floorDiv(value, unit), fraction, ZoneOffset.UTC);
        return local.format(DateTimeFormatter.ofPattern(kind.nanos
                ? "uuuu-MM-dd HH:mm:ss.SSSSSSSSS" : "uuuu-MM-dd HH:mm:ss.SSSSSS"));
    }

    private static String display(long value, Kind kind, ZoneId zone) {
        if (!kind.instant) return civil(value, kind);
        long unit = kind.nanos ? 1000000000L : 1000000L;
        long fraction = Math.floorMod(value, unit) * (kind.nanos ? 1L : 1000L);
        return Instant.ofEpochSecond(Math.floorDiv(value, unit), fraction).atZone(zone)
                .format(DateTimeFormatter.ofPattern(kind.nanos
                        ? "uuuu-MM-dd HH:mm:ss.SSSSSSSSS" : "uuuu-MM-dd HH:mm:ss.SSSSSS"));
    }

    private static Map<String, Object> timeOracle(Long raw, Kind kind) {
        if (raw == null) return null;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("encoded", raw);
        result.put("utc", display(raw, kind, ZoneOffset.UTC));
        result.put("america_new_york", display(raw, kind, NEW_YORK));
        return result;
    }

    private static void same(GenericRecord expected, GenericRecord actual) {
        for (Schema.Field field : expected.getSchema().getFields()) {
            Object left = expected.get(field.name());
            Object right = actual.get(field.name());
            if (left instanceof GenericRecord nested) {
                require(right instanceof GenericRecord, "Lost nested record " + field.name());
                same(nested, (GenericRecord) right);
            } else if (left instanceof Number number) {
                require(right instanceof Number && number.longValue() == ((Number) right).longValue(),
                        "Changed numeric value " + field.name());
            } else if (left instanceof CharSequence text) {
                require(right instanceof CharSequence && text.toString().equals(right.toString()),
                        "Changed text value " + field.name());
            } else {
                require(Objects.equals(left, right), "Changed null value " + field.name());
            }
        }
    }

    private static Map<String, Object> write(Path output, String fileName,
            org.apache.iceberg.Schema iceberg, List<GenericData.Record> expected,
            List<Map<String, Object>> oracle, Kind kind, boolean deleteShape) throws Exception {
        Path file = output.resolve(fileName);
        require(!Files.exists(file), "Refusing to replace an existing fixture");
        // Avro.write owns schema conversion, OCF framing and all timestamp annotations.
        try (FileAppender<GenericData.Record> appender = Avro.write(org.apache.iceberg.Files.localOutput(file.toFile()))
                .schema(iceberg).named(ROOT).set("write.avro.compression-codec", "uncompressed")
                .createWriterFunc(GenericAvroWriter::create).build()) {
            appender.addAll(expected);
        }
        Schema written;
        String storedIcebergSchema;
        try (DataFileReader<GenericRecord> header = new DataFileReader<>(file.toFile(), new GenericDatumReader<>())) {
            written = header.getSchema();
            storedIcebergSchema = header.getMetaString("iceberg.schema");
        }
        require(SchemaParser.toJson(iceberg).equals(storedIcebergSchema), "Missing Iceberg schema metadata");
        if (deleteShape) {
            require(AvroSchemaUtil.getFieldId(written.getField("file_path")) == 2147483546, "Wrong path field id");
            require(AvroSchemaUtil.getFieldId(written.getField("pos")) == 2147483545, "Wrong position field id");
            require(AvroSchemaUtil.getFieldId(written.getField("row")) == 2147483544, "Wrong row field id");
            assertClock(nonNull(written.getField("row").schema()).getField("clock").schema(), kind);
        } else {
            require(AvroSchemaUtil.getFieldId(written.getField("id")) == 1, "Wrong id field id");
            require(AvroSchemaUtil.getFieldId(written.getField("clock")) == 2, "Wrong clock field id");
            require(AvroSchemaUtil.getFieldId(written.getField("payload")) == 3, "Wrong payload field id");
            Schema payload = nonNull(written.getField("payload").schema());
            require(AvroSchemaUtil.getFieldId(payload.getField("clock")) == 4, "Wrong nested clock field id");
            assertClock(written.getField("clock").schema(), kind);
            assertClock(payload.getField("clock").schema(), kind);
        }
        int count = 0;
        try (CloseableIterable<GenericRecord> records = Avro.read(org.apache.iceberg.Files.localInput(file.toFile()))
                .project(iceberg).createReaderFunc(schema -> GenericAvroReader.<GenericRecord>create(schema)).build()) {
            for (GenericRecord actual : records) {
                require(count < expected.size(), "Too many records from Iceberg reader");
                same(expected.get(count), actual);
                count++;
            }
        }
        require(count == expected.size(), "Too few records from Iceberg reader");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("file", fileName);
        result.put("bytes", Files.size(file));
        result.put("sha256", sha256(Files.readAllBytes(file)));
        result.put("record_count", count);
        result.put("iceberg_schema", JSON.readTree(SchemaParser.toJson(iceberg)));
        result.put("avro_schema", JSON.readTree(written.toString()));
        result.put("logical_type", kind.logical());
        result.put("adjust_to_utc", kind.instant);
        result.put("oracle", oracle);
        return result;
    }

    private static Map<String, Object> data(Path output, Kind kind) throws Exception {
        org.apache.iceberg.Schema iceberg = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "clock", kind.type()),
                Types.NestedField.optional(3, "payload", Types.StructType.of(
                        Types.NestedField.optional(4, "clock", kind.type()))));
        Schema avro = AvroSchemaUtil.convert(iceberg, ROOT);
        Schema payloadSchema = nonNull(avro.getField("payload").schema());
        List<GenericData.Record> records = new ArrayList<>();
        List<Map<String, Object>> oracle = new ArrayList<>();
        long[] values = kind.encodedValues();
        for (int i = 0; i < 8; i++) {
            Long raw = i < values.length ? values[i] : null;
            GenericData.Record record = new GenericData.Record(avro);
            record.put("id", i + 1);
            record.put("clock", raw);
            GenericData.Record payload = null;
            Map<String, Object> payloadOracle = null;
            if (NESTED[i] != -1) {
                Long nested = NESTED[i] == -2 ? null : values[NESTED[i]];
                payload = new GenericData.Record(payloadSchema);
                payload.put("clock", nested);
                payloadOracle = new LinkedHashMap<>();
                payloadOracle.put("clock", timeOracle(nested, kind));
            }
            record.put("payload", payload);
            records.add(record);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", i + 1);
            row.put("clock", timeOracle(raw, kind));
            row.put("payload", payloadOracle);
            oracle.add(row);
        }
        return write(output, kind.file(), iceberg, records, oracle, kind, false);
    }

    private static Map<String, Object> deletes(Path output) throws Exception {
        org.apache.iceberg.Schema iceberg = new org.apache.iceberg.Schema(
                Types.NestedField.required(2147483546, "file_path", Types.StringType.get()),
                Types.NestedField.required(2147483545, "pos", Types.LongType.get()),
                Types.NestedField.optional(2147483544, "row", Types.StructType.of(
                        Types.NestedField.optional(2, "clock", Types.TimestampNanoType.withoutZone()))));
        Schema avro = AvroSchemaUtil.convert(iceberg, ROOT);
        Schema rowSchema = nonNull(avro.getField("row").schema());
        List<GenericData.Record> records = new ArrayList<>();
        List<Map<String, Object>> oracle = new ArrayList<>();
        long[] positions = {0L, 2L};
        long[] times = {0L, -1L};
        for (int i = 0; i < positions.length; i++) {
            GenericData.Record row = new GenericData.Record(rowSchema);
            row.put("clock", times[i]);
            GenericData.Record record = new GenericData.Record(avro);
            record.put("file_path", "data.avro");
            record.put("pos", positions[i]);
            record.put("row", row);
            records.add(record);
            Map<String, Object> expected = new LinkedHashMap<>();
            expected.put("file_path", "data.avro");
            expected.put("pos", positions[i]);
            expected.put("row", Map.of("clock", timeOracle(times[i], Kind.CIVIL_NANOS)));
            oracle.add(expected);
        }
        return write(output, "iceberg_position_deletes.avro", iceberg, records, oracle, Kind.CIVIL_NANOS, true);
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "Usage: GenerateIcebergTimestamps <new-output-directory>");
        Path output = Path.of(args[0]);
        Files.createDirectories(output);
        List<Map<String, Object>> origins = new ArrayList<>();
        origins.add(origin(Avro.class, "iceberg-core-1.12.0.jar"));
        origins.add(origin(AvroSchemaUtil.class, "iceberg-core-1.12.0.jar"));
        origins.add(origin(GenericAvroWriter.class, "iceberg-core-1.12.0.jar"));
        origins.add(origin(GenericAvroReader.class, "iceberg-core-1.12.0.jar"));
        origins.add(origin(Types.TimestampNanoType.class, "iceberg-api-1.12.0.jar"));
        origins.add(origin(Schema.class, "avro-1.12.1.jar"));
        List<Map<String, Object>> files = new ArrayList<>();
        for (Kind kind : Kind.values()) files.add(data(output, kind));
        files.add(deletes(output));
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("producer", "Apache Iceberg 1.12.0 Avro.write + GenericAvroWriter");
        manifest.put("avro", "1.12.1 (Doris repository pin)");
        manifest.put("codec", "uncompressed");
        manifest.put("origins", origins);
        manifest.put("files", files);
        manifest.put("reproduction", "OCF sync markers are producer-generated; record/schema oracles are stable, file SHA256 may vary across runs.");
        byte[] json = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest);
        Files.write(output.resolve("manifest.json"), json);
        Files.writeString(output.resolve("manifest.sha256"), sha256(json) + "  manifest.json\n");
        System.out.println("PASS: generated and independently read back five Iceberg-produced OCF fixtures");
    }
}

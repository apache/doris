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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;

/** Offline generator and verifier for the VariantEncoding v1 golden corpus. */
public final class ParquetVariantGolden {
    private static final String ARTIFACT = "org.apache.parquet:parquet-variant:1.17.0";
    private static final String JAR_SHA256 =
            "daecf8161e7bba63f7ba9fd62c1e8a77730c9a9d76a335191dc9d0a0fcaaec52";
    private static final HexFormat HEX = HexFormat.of();
    private static final Comparator<String> UNSIGNED_UTF8 =
            (left, right) -> Arrays.compareUnsigned(utf8(left), utf8(right));

    private ParquetVariantGolden() {}

    @FunctionalInterface
    private interface Appender {
        void append(VariantBuilder builder);
    }

    private record GoldenVector(
            String name,
            String provenance,
            String rootType,
            String expected,
            byte[] metadata,
            byte[] value) {}

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("generate")) {
            knownIncompatibilityCheck();
            Path output = Path.of(args[1]);
            Files.createDirectories(output);
            writeCorpus(
                    output.resolve("parquet_java_vectors.tsv"),
                    "java-to-doris",
                    javaToDorisVectors());
            writeCorpus(
                    output.resolve("doris_java_verified_vectors.tsv"),
                    "doris-to-java",
                    dorisToJavaVectors());
            return;
        }
        if (args.length == 1 && args[0].equals("extended")) {
            knownIncompatibilityCheck();
            verifyRealFourByteBoundaries();
            return;
        }
        throw new IllegalArgumentException(
                "Usage: ParquetVariantGolden generate <output-dir> | extended");
    }

    private static List<GoldenVector> javaToDorisVectors() {
        List<GoldenVector> vectors = new ArrayList<>();
        vectors.add(builderVector("primitive_null", "null", VariantBuilder::appendNull));
        vectors.add(builderVector("primitive_true", "bool:true", b -> b.appendBoolean(true)));
        vectors.add(builderVector("primitive_false", "bool:false", b -> b.appendBoolean(false)));
        vectors.add(builderVector("primitive_int8", "int8:-7", b -> b.appendByte((byte) -7)));
        vectors.add(builderVector("primitive_int16", "int16:128", b -> b.appendShort((short) 128)));
        vectors.add(builderVector("primitive_int32", "int32:32768", b -> b.appendInt(32768)));
        vectors.add(
                builderVector(
                        "primitive_int64", "int64:2147483648", b -> b.appendLong(2147483648L)));
        vectors.add(
                builderVector(
                        "primitive_double",
                        "double:400921fb54442d18",
                        b -> b.appendDouble(Math.PI)));
        vectors.add(
                builderVector(
                        "primitive_decimal4",
                        "decimal4:1234567:2",
                        b -> b.appendDecimal(new BigDecimal("12345.67"))));
        vectors.add(
                builderVector(
                        "primitive_decimal8",
                        "decimal8:123456789012345:5",
                        b -> b.appendDecimal(new BigDecimal("1234567890.12345"))));
        vectors.add(
                builderVector(
                        "primitive_decimal16",
                        "decimal16:12345678901234567891234567890:10",
                        b -> b.appendDecimal(new BigDecimal("1234567890123456789.1234567890"))));
        vectors.add(builderVector("primitive_date", "date:-12345", b -> b.appendDate(-12345)));
        vectors.add(
                builderVector(
                        "primitive_timestamp_tz",
                        "timestamp_tz:-1234567890123",
                        b -> b.appendTimestampTz(-1234567890123L)));
        vectors.add(
                builderVector(
                        "primitive_timestamp_ntz",
                        "timestamp_ntz:2234567890123",
                        b -> b.appendTimestampNtz(2234567890123L)));
        vectors.add(
                builderVector(
                        "primitive_float", "float:3fc00000", b -> b.appendFloat(1.5F)));
        vectors.add(
                builderVector(
                        "primitive_binary",
                        "binary:0001ff",
                        b -> b.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, (byte) 0xFF}))));
        String longString = "L".repeat(64);
        vectors.add(
                builderVector(
                        "primitive_long_string",
                        "string:" + hexUtf8(longString),
                        b -> b.appendString(longString)));
        vectors.add(
                builderVector(
                        "primitive_time",
                        "time:86399999999",
                        b -> b.appendTime(86_399_999_999L)));
        vectors.add(
                builderVector(
                        "primitive_timestamp_nanos_tz",
                        "timestamp_nanos_tz:-3234567890123",
                        b -> b.appendTimestampNanosTz(-3234567890123L)));
        vectors.add(
                builderVector(
                        "primitive_timestamp_nanos_ntz",
                        "timestamp_nanos_ntz:4234567890123",
                        b -> b.appendTimestampNanosNtz(4234567890123L)));
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        vectors.add(
                builderVector(
                        "primitive_uuid", "uuid:" + uuid, b -> b.appendUUID(uuid)));

        vectors.add(builderVector("short_string_empty", "string:", b -> b.appendString("")));
        String short63 = "s".repeat(63);
        vectors.add(
                builderVector(
                        "short_string_63",
                        "string:" + hexUtf8(short63),
                        b -> b.appendString(short63)));
        String unicode = "A\u00E9\u4E2D\uD800\uDC00";
        vectors.add(
                builderVector(
                        "short_string_unicode",
                        "string:" + hexUtf8(unicode),
                        b -> b.appendString(unicode)));

        vectors.add(
                builderVector(
                        "empty_object",
                        "object{}",
                        builder -> {
                            builder.startObject();
                            builder.endObject();
                        }));
        vectors.add(
                builderVector(
                        "empty_array",
                        "array[]",
                        builder -> {
                            builder.startArray();
                            builder.endArray();
                        }));
        vectors.add(normalObjectVector());
        vectors.add(normalArrayVector());
        vectors.add(normalNestedVector());
        vectors.add(unicodeBmpObjectVector());
        vectors.add(unicodeSupplementaryObjectVector());

        vectors.add(metadataBoundaryVector(255));
        vectors.add(metadataBoundaryVector(256));
        vectors.add(metadataBoundaryVector(65_535));
        vectors.add(metadataBoundaryVector(65_536));
        vectors.add(offsetBoundaryVector(255));
        vectors.add(offsetBoundaryVector(256));
        vectors.add(offsetBoundaryVector(65_535));
        vectors.add(offsetBoundaryVector(65_536));
        vectors.add(arrayCountBoundaryVector(255));
        vectors.add(arrayCountBoundaryVector(256));
        vectors.add(objectIdBoundaryVector(256));
        vectors.add(objectIdBoundaryVector(257));

        vectors.add(rawMetadataWidthFourVector());
        vectors.add(rawArrayOffsetWidthFourVector());
        vectors.add(rawObjectIdWidthVector(3));
        vectors.add(rawObjectIdWidthVector(4));
        vectors.add(rawNonMonotonicObjectVector());
        return vectors;
    }

    private static List<GoldenVector> dorisToJavaVectors() {
        List<GoldenVector> vectors = new ArrayList<>();
        vectors.add(canonicalVector("doris_null", "null", List.of(), VariantBuilder::appendNull));
        vectors.add(
                canonicalVector(
                        "doris_true", "bool:true", List.of(), b -> b.appendBoolean(true)));
        vectors.add(
                canonicalVector(
                        "doris_false", "bool:false", List.of(), b -> b.appendBoolean(false)));
        vectors.add(
                canonicalVector(
                        "doris_int8", "int8:-7", List.of(), b -> b.appendByte((byte) -7)));
        vectors.add(
                canonicalVector(
                        "doris_int16", "int16:128", List.of(), b -> b.appendShort((short) 128)));
        vectors.add(
                canonicalVector(
                        "doris_int32", "int32:32768", List.of(), b -> b.appendInt(32768)));
        vectors.add(
                canonicalVector(
                        "doris_int64",
                        "int64:2147483648",
                        List.of(),
                        b -> b.appendLong(2147483648L)));
        vectors.add(
                canonicalVector(
                        "doris_double",
                        "double:400921fb54442d18",
                        List.of(),
                        b -> b.appendDouble(Math.PI)));
        vectors.add(
                canonicalVector(
                        "doris_decimal4",
                        "decimal4:1234567:2",
                        List.of(),
                        b -> b.appendDecimal(new BigDecimal("12345.67"))));
        vectors.add(
                canonicalVector(
                        "doris_decimal8",
                        "decimal8:123456789012345:5",
                        List.of(),
                        b -> b.appendDecimal(new BigDecimal("1234567890.12345"))));
        vectors.add(
                canonicalVector(
                        "doris_decimal16",
                        "decimal16:12345678901234567891234567890:10",
                        List.of(),
                        b -> b.appendDecimal(new BigDecimal("1234567890123456789.1234567890"))));
        vectors.add(
                canonicalVector(
                        "doris_date", "date:-12345", List.of(), b -> b.appendDate(-12345)));
        vectors.add(
                canonicalVector(
                        "doris_timestamp_tz",
                        "timestamp_tz:-1234567890123",
                        List.of(),
                        b -> b.appendTimestampTz(-1234567890123L)));
        vectors.add(
                canonicalVector(
                        "doris_timestamp_ntz",
                        "timestamp_ntz:2234567890123",
                        List.of(),
                        b -> b.appendTimestampNtz(2234567890123L)));
        vectors.add(
                canonicalVector(
                        "doris_float",
                        "float:3fc00000",
                        List.of(),
                        b -> b.appendFloat(1.5F)));
        vectors.add(
                canonicalVector(
                        "doris_binary",
                        "binary:0001ff",
                        List.of(),
                        b -> b.appendBinary(ByteBuffer.wrap(new byte[] {0, 1, (byte) 0xFF}))));
        String longString = "L".repeat(64);
        vectors.add(
                canonicalVector(
                        "doris_long_string",
                        "string:" + hexUtf8(longString),
                        List.of(),
                        b -> b.appendString(longString)));
        vectors.add(
                canonicalVector(
                        "doris_time",
                        "time:86399999999",
                        List.of(),
                        b -> b.appendTime(86_399_999_999L)));
        vectors.add(
                canonicalVector(
                        "doris_timestamp_nanos_tz",
                        "timestamp_nanos_tz:-3234567890123",
                        List.of(),
                        b -> b.appendTimestampNanosTz(-3234567890123L)));
        vectors.add(
                canonicalVector(
                        "doris_timestamp_nanos_ntz",
                        "timestamp_nanos_ntz:4234567890123",
                        List.of(),
                        b -> b.appendTimestampNanosNtz(4234567890123L)));
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        vectors.add(
                canonicalVector(
                        "doris_uuid", "uuid:" + uuid, List.of(), b -> b.appendUUID(uuid)));
        vectors.add(
                canonicalVector(
                        "doris_short_empty", "string:", List.of(), b -> b.appendString("")));
        String short63 = "s".repeat(63);
        vectors.add(
                canonicalVector(
                        "doris_short_63",
                        "string:" + hexUtf8(short63),
                        List.of(),
                        b -> b.appendString(short63)));
        String unicode = "A\u00E9\u4E2D\uD800\uDC00";
        vectors.add(
                canonicalVector(
                        "doris_unicode_string",
                        "string:" + hexUtf8(unicode),
                        List.of(),
                        b -> b.appendString(unicode)));
        vectors.add(canonicalObjectVector());
        vectors.add(canonicalArrayVector());
        vectors.add(canonicalNestedVector());
        vectors.add(canonicalUnicodeObjectVector());
        return vectors;
    }

    private static GoldenVector builderVector(String name, String expected, Appender append) {
        VariantBuilder builder = new VariantBuilder();
        append.append(builder);
        GoldenVector vector =
                checkedVector(name, "parquet-java-builder", expected, builder.build());
        if ((vector.metadata()[0] & 0x10) != 0) {
            throw new IllegalStateException("MetadataBuilder unexpectedly set sorted_strings");
        }
        return vector;
    }

    private static GoldenVector canonicalVector(
            String name, String expected, List<String> keys, Appender append) {
        List<String> sortedKeys = new ArrayList<>(keys);
        sortedKeys.sort(UNSIGNED_UTF8);
        if (new HashSet<>(sortedKeys).size() != sortedKeys.size()) {
            throw new IllegalArgumentException("Duplicate canonical metadata key for " + name);
        }
        byte[] encodedMetadata = encodeMetadata(sortedKeys, true, 0);
        ImmutableMetadata metadata = new ImmutableMetadata(ByteBuffer.wrap(encodedMetadata));
        for (int index = 0; index < sortedKeys.size(); ++index) {
            if (metadata.getOrInsert(sortedKeys.get(index)) != index) {
                throw new IllegalStateException("Immutable metadata id mismatch for " + name);
            }
        }
        VariantBuilder builder = new VariantBuilder(metadata);
        append.append(builder);
        Variant variant = builder.build();
        if (!Arrays.equals(encodedMetadata, bytes(variant.getMetadataBuffer()))) {
            throw new IllegalStateException("Canonical metadata changed for " + name);
        }
        return checkedVector(name, "doris-canonical-java-validated", expected, variant);
    }

    private static GoldenVector checkedVector(
            String name, String provenance, String expected, Variant variant) {
        String actual = describe(variant);
        if (!actual.equals(expected)) {
            throw new IllegalStateException(
                    "Typed accessor mismatch for " + name + ": expected " + expected + ", got " + actual);
        }
        return new GoldenVector(
                name,
                provenance,
                rootType(variant),
                expected,
                bytes(variant.getMetadataBuffer()),
                bytes(variant.getValueBuffer()));
    }

    private static GoldenVector checkedRawVector(
            String name, String expected, byte[] metadata, byte[] value) {
        ImmutableMetadata parsed = new ImmutableMetadata(ByteBuffer.wrap(metadata));
        if (!Arrays.equals(metadata, bytes(parsed.getEncodedBuffer()))) {
            throw new IllegalStateException("ImmutableMetadata changed raw bytes for " + name);
        }
        return checkedVector(
                name,
                "spec-raw-java-validated",
                expected,
                new Variant(value, metadata));
    }

    private static GoldenVector normalObjectVector() {
        String expected = "object{61=string:78;7a=int32:1}";
        return builderVector(
                "object_unsorted_metadata",
                expected,
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey("z");
                    object.appendInt(1);
                    object.appendKey("a");
                    object.appendString("x");
                    builder.endObject();
                });
    }

    private static GoldenVector normalArrayVector() {
        return builderVector(
                "array_ordered",
                "array[int8:1;string:78;bool:false]",
                builder -> {
                    VariantArrayBuilder array = builder.startArray();
                    array.appendByte((byte) 1);
                    array.appendString("x");
                    array.appendBoolean(false);
                    builder.endArray();
                });
    }

    private static GoldenVector normalNestedVector() {
        String expected = "object{617272=array[int8:7;object{696e73696465=bool:false}]}";
        return builderVector(
                "nested_object_array",
                expected,
                builder -> {
                    VariantObjectBuilder root = builder.startObject();
                    root.appendKey("arr");
                    VariantArrayBuilder array = root.startArray();
                    array.appendByte((byte) 7);
                    VariantObjectBuilder inner = array.startObject();
                    inner.appendKey("inside");
                    inner.appendBoolean(false);
                    array.endObject();
                    root.endArray();
                    builder.endObject();
                });
    }

    private static GoldenVector unicodeBmpObjectVector() {
        String first = "\u00E9";
        String second = "\uE000";
        String expected =
                "object{" + hexUtf8(first) + "=int32:1;" + hexUtf8(second) + "=int32:2}";
        return builderVector(
                "unicode_bmp_order",
                expected,
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey(second);
                    object.appendInt(2);
                    object.appendKey(first);
                    object.appendInt(1);
                    builder.endObject();
                });
    }

    private static GoldenVector unicodeSupplementaryObjectVector() {
        String first = "\uD800\uDC00";
        String second = "\uD800\uDC01";
        String expected =
                "object{" + hexUtf8(first) + "=int32:1;" + hexUtf8(second) + "=int32:2}";
        return builderVector(
                "unicode_supplementary_order",
                expected,
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey(second);
                    object.appendInt(2);
                    object.appendKey(first);
                    object.appendInt(1);
                    builder.endObject();
                });
    }

    private static GoldenVector metadataBoundaryVector(int keyBytes) {
        String key = "k".repeat(keyBytes);
        GoldenVector vector =
                builderVector(
                "metadata_bytes_" + keyBytes,
                "object{" + hexUtf8(key) + "=null}",
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey(key);
                    object.appendNull();
                    builder.endObject();
                });
        int expectedWidth = minUnsignedWidth(keyBytes);
        int actualWidth = ((vector.metadata()[0] >>> 6) & 0x03) + 1;
        if (actualWidth != expectedWidth) {
            throw new IllegalStateException("Metadata width boundary mismatch at " + keyBytes);
        }
        return vector;
    }

    private static GoldenVector offsetBoundaryVector(int encodedChildBytes) {
        byte[] payload = new byte[encodedChildBytes - 5];
        Arrays.fill(payload, (byte) 0xAB);
        GoldenVector vector =
                builderVector(
                "array_offset_bytes_" + encodedChildBytes,
                "array[binary:" + HEX.formatHex(payload) + "]",
                builder -> {
                    VariantArrayBuilder array = builder.startArray();
                    array.appendBinary(ByteBuffer.wrap(payload));
                    builder.endArray();
                });
        int valueHeader = (vector.value()[0] & 0xFF) >>> 2;
        int actualWidth = (valueHeader & 0x03) + 1;
        if (actualWidth != minUnsignedWidth(encodedChildBytes)) {
            throw new IllegalStateException(
                    "Array offset width boundary mismatch at " + encodedChildBytes);
        }
        return vector;
    }

    private static GoldenVector arrayCountBoundaryVector(int count) {
        GoldenVector vector =
                builderVector(
                "array_count_" + count,
                repeatedArrayExpected(count),
                builder -> {
                    VariantArrayBuilder array = builder.startArray();
                    for (int index = 0; index < count; ++index) {
                        array.appendNull();
                    }
                    builder.endArray();
                });
        int valueHeader = (vector.value()[0] & 0xFF) >>> 2;
        boolean isLarge = (valueHeader & 0x04) != 0;
        if (isLarge != (count > 255)) {
            throw new IllegalStateException("Array count boundary mismatch at " + count);
        }
        return vector;
    }

    private static GoldenVector objectIdBoundaryVector(int count) {
        List<String> keys = numberedKeys(count);
        GoldenVector vector =
                builderVector(
                "object_id_count_" + count,
                nullObjectExpected(keys),
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    for (String key : keys) {
                        object.appendKey(key);
                        object.appendNull();
                    }
                    builder.endObject();
                });
        int valueHeader = (vector.value()[0] & 0xFF) >>> 2;
        int idWidth = ((valueHeader >>> 2) & 0x03) + 1;
        boolean isLarge = (valueHeader & 0x10) != 0;
        if (idWidth != minUnsignedWidth(count - 1) || isLarge != (count > 255)) {
            throw new IllegalStateException("Object id/count boundary mismatch at " + count);
        }
        return vector;
    }

    private static GoldenVector rawMetadataWidthFourVector() {
        byte[] metadata = encodeMetadata(List.of(), true, 4);
        return checkedRawVector(
                "raw_metadata_width_4", "null", metadata, new byte[] {0});
    }

    private static GoldenVector rawArrayOffsetWidthFourVector() {
        byte[] metadata = encodeMetadata(List.of(), true, 1);
        byte[] value = rawArray(4, List.of(new byte[] {0}));
        return checkedRawVector("raw_array_offset_width_4", "array[null]", metadata, value);
    }

    private static GoldenVector rawObjectIdWidthVector(int idWidth) {
        byte[] metadata = encodeMetadata(List.of("a"), true, 1);
        byte[] value = rawObject(idWidth, 1, new int[] {0}, new int[] {0, 1}, new byte[] {0});
        return checkedRawVector(
                "raw_object_id_width_" + idWidth, "object{61=null}", metadata, value);
    }

    private static GoldenVector rawNonMonotonicObjectVector() {
        byte[] metadata = encodeMetadata(List.of("a", "b"), true, 1);
        byte[] shortX = shortString("x");
        byte[] intOne = new byte[] {(byte) (3 << 2), 1};
        byte[] values = concat(shortX, intOne);
        byte[] value = rawObject(1, 1, new int[] {0, 1}, new int[] {2, 0, 4}, values);
        return checkedRawVector(
                "raw_object_nonmonotonic_offsets",
                "object{61=int8:1;62=string:78}",
                metadata,
                value);
    }

    private static GoldenVector canonicalObjectVector() {
        return canonicalVector(
                "doris_object",
                "object{61=string:78;7a=int8:1}",
                List.of("z", "a"),
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey("z");
                    object.appendByte((byte) 1);
                    object.appendKey("a");
                    object.appendString("x");
                    builder.endObject();
                });
    }

    private static GoldenVector canonicalArrayVector() {
        return canonicalVector(
                "doris_array",
                "array[int8:1;string:78;bool:false]",
                List.of(),
                builder -> {
                    VariantArrayBuilder array = builder.startArray();
                    array.appendByte((byte) 1);
                    array.appendString("x");
                    array.appendBoolean(false);
                    builder.endArray();
                });
    }

    private static GoldenVector canonicalNestedVector() {
        return canonicalVector(
                "doris_nested",
                "object{617272=array[int8:7;object{696e73696465=bool:false}]}",
                List.of("arr", "inside"),
                builder -> {
                    VariantObjectBuilder root = builder.startObject();
                    root.appendKey("arr");
                    VariantArrayBuilder array = root.startArray();
                    array.appendByte((byte) 7);
                    VariantObjectBuilder inner = array.startObject();
                    inner.appendKey("inside");
                    inner.appendBoolean(false);
                    array.endObject();
                    root.endArray();
                    builder.endObject();
                });
    }

    private static GoldenVector canonicalUnicodeObjectVector() {
        String first = "\u00E9";
        String second = "\uE000";
        return canonicalVector(
                "doris_unicode_object",
                "object{" + hexUtf8(first) + "=int8:1;" + hexUtf8(second) + "=int8:2}",
                List.of(second, first),
                builder -> {
                    VariantObjectBuilder object = builder.startObject();
                    object.appendKey(second);
                    object.appendByte((byte) 2);
                    object.appendKey(first);
                    object.appendByte((byte) 1);
                    builder.endObject();
                });
    }

    private static String describe(Variant variant) {
        return switch (variant.getType()) {
            case NULL -> "null";
            case BOOLEAN -> "bool:" + variant.getBoolean();
            case BYTE -> "int8:" + variant.getByte();
            case SHORT -> "int16:" + variant.getShort();
            case INT -> "int32:" + variant.getInt();
            case LONG -> "int64:" + variant.getLong();
            case DOUBLE ->
                    "double:"
                            + String.format(
                                    Locale.ROOT,
                                    "%016x",
                                    Double.doubleToRawLongBits(variant.getDouble()));
            case DECIMAL4 -> decimalDescription("decimal4", variant.getDecimal());
            case DECIMAL8 -> decimalDescription("decimal8", variant.getDecimal());
            case DECIMAL16 -> decimalDescription("decimal16", variant.getDecimal());
            case DATE -> "date:" + variant.getInt();
            case TIMESTAMP_TZ -> "timestamp_tz:" + variant.getLong();
            case TIMESTAMP_NTZ -> "timestamp_ntz:" + variant.getLong();
            case FLOAT ->
                    "float:"
                            + String.format(
                                    Locale.ROOT,
                                    "%08x",
                                    Float.floatToRawIntBits(variant.getFloat()));
            case BINARY -> "binary:" + HEX.formatHex(bytes(variant.getBinary()));
            case STRING -> "string:" + hexUtf8(variant.getString());
            case TIME -> "time:" + variant.getLong();
            case TIMESTAMP_NANOS_TZ -> "timestamp_nanos_tz:" + variant.getLong();
            case TIMESTAMP_NANOS_NTZ -> "timestamp_nanos_ntz:" + variant.getLong();
            case UUID -> "uuid:" + variant.getUUID().toString().toLowerCase(Locale.ROOT);
            case OBJECT -> describeObject(variant);
            case ARRAY -> describeArray(variant);
        };
    }

    private static String decimalDescription(String type, BigDecimal decimal) {
        return type + ":" + decimal.unscaledValue() + ":" + decimal.scale();
    }

    private static String describeObject(Variant variant) {
        StringBuilder result = new StringBuilder("object{");
        for (int index = 0; index < variant.numObjectElements(); ++index) {
            if (index != 0) {
                result.append(';');
            }
            Variant.ObjectField field = variant.getFieldAtIndex(index);
            Variant byKey = variant.getFieldByKey(field.key);
            if (byKey == null || !describe(byKey).equals(describe(field.value))) {
                throw new IllegalStateException("Official object lookup disagrees with iteration");
            }
            result.append(hexUtf8(field.key)).append('=').append(describe(field.value));
        }
        return result.append('}').toString();
    }

    private static String describeArray(Variant variant) {
        StringBuilder result = new StringBuilder("array[");
        for (int index = 0; index < variant.numArrayElements(); ++index) {
            if (index != 0) {
                result.append(';');
            }
            result.append(describe(variant.getElementAtIndex(index)));
        }
        return result.append(']').toString();
    }

    private static String rootType(Variant variant) {
        return switch (variant.getType()) {
            case NULL -> "null";
            case BOOLEAN -> "boolean";
            case BYTE -> "byte";
            case SHORT -> "short";
            case INT -> "int";
            case LONG -> "long";
            case STRING -> "string";
            case DOUBLE -> "double";
            case DECIMAL4 -> "decimal4";
            case DECIMAL8 -> "decimal8";
            case DECIMAL16 -> "decimal16";
            case DATE -> "date";
            case TIMESTAMP_TZ -> "timestamp_tz";
            case TIMESTAMP_NTZ -> "timestamp_ntz";
            case FLOAT -> "float";
            case BINARY -> "binary";
            case TIME -> "time";
            case TIMESTAMP_NANOS_TZ -> "timestamp_nanos_tz";
            case TIMESTAMP_NANOS_NTZ -> "timestamp_nanos_ntz";
            case UUID -> "uuid";
            case OBJECT -> "object";
            case ARRAY -> "array";
        };
    }

    private static void writeCorpus(Path file, String corpus, List<GoldenVector> vectors)
            throws Exception {
        vectors.sort(Comparator.comparing(GoldenVector::name));
        Set<String> names = new HashSet<>();
        StringBuilder output = new StringBuilder();
        output.append("# Licensed to the Apache Software Foundation (ASF) under one\n");
        output.append("# or more contributor license agreements.  See the NOTICE file\n");
        output.append("# distributed with this work for additional information\n");
        output.append("# regarding copyright ownership.  The ASF licenses this file\n");
        output.append("# to you under the Apache License, Version 2.0 (the\n");
        output.append("# \"License\"); you may not use this file except in compliance\n");
        output.append("# with the License.  You may obtain a copy of the License at\n");
        output.append("#\n");
        output.append("#   http://www.apache.org/licenses/LICENSE-2.0\n");
        output.append("#\n");
        output.append("# Unless required by applicable law or agreed to in writing,\n");
        output.append("# software distributed under the License is distributed on an\n");
        output.append("# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n");
        output.append("# KIND, either express or implied.  See the License for the\n");
        output.append("# specific language governing permissions and limitations\n");
        output.append("# under the License.\n");
        output.append("#\n");
        output.append("# variant-golden-v1\n");
        output.append("# artifact=").append(ARTIFACT).append('\n');
        output.append("# jar_sha256=").append(JAR_SHA256).append('\n');
        output.append("# corpus=").append(corpus).append('\n');
        output.append("# columns=name\\tprovenance\\troot_type\\texpected\\tmetadata_hex\\tvalue_hex\n");
        for (GoldenVector vector : vectors) {
            if (!names.add(vector.name())) {
                throw new IllegalStateException("Duplicate vector name " + vector.name());
            }
            output.append(vector.name())
                    .append('\t')
                    .append(vector.provenance())
                    .append('\t')
                    .append(vector.rootType())
                    .append('\t')
                    .append(vector.expected())
                    .append('\t')
                    .append(HEX.formatHex(vector.metadata()))
                    .append('\t')
                    .append(HEX.formatHex(vector.value()))
                    .append('\n');
        }
        Files.writeString(file, output, StandardCharsets.UTF_8);
    }

    private static byte[] encodeMetadata(List<String> keys, boolean sorted, int forcedWidth) {
        List<byte[]> encodedKeys = keys.stream().map(ParquetVariantGolden::utf8).toList();
        long stringsSize = encodedKeys.stream().mapToLong(bytes -> bytes.length).sum();
        int width =
                forcedWidth == 0
                        ? minUnsignedWidth(Math.max(keys.size(), stringsSize))
                        : forcedWidth;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(1 | (sorted ? 0x10 : 0) | ((width - 1) << 6));
        writeUnsigned(output, keys.size(), width);
        long offset = 0;
        writeUnsigned(output, offset, width);
        for (byte[] key : encodedKeys) {
            offset += key.length;
            writeUnsigned(output, offset, width);
        }
        for (byte[] key : encodedKeys) {
            output.writeBytes(key);
        }
        return output.toByteArray();
    }

    private static byte[] rawArray(int offsetWidth, List<byte[]> children) {
        ByteArrayOutputStream values = new ByteArrayOutputStream();
        int[] offsets = new int[children.size() + 1];
        for (int index = 0; index < children.size(); ++index) {
            offsets[index] = values.size();
            values.writeBytes(children.get(index));
        }
        offsets[children.size()] = values.size();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(3 | ((offsetWidth - 1) << 2));
        output.write(children.size());
        for (int offset : offsets) {
            writeUnsigned(output, offset, offsetWidth);
        }
        output.writeBytes(values.toByteArray());
        return output.toByteArray();
    }

    private static byte[] rawObject(
            int idWidth,
            int offsetWidth,
            int[] ids,
            int[] offsets,
            byte[] values) {
        if (offsets.length != ids.length + 1 || ids.length > 255) {
            throw new IllegalArgumentException("Invalid compact raw object shape");
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(2 | ((offsetWidth - 1) << 2) | ((idWidth - 1) << 4));
        output.write(ids.length);
        for (int id : ids) {
            writeUnsigned(output, id, idWidth);
        }
        for (int offset : offsets) {
            writeUnsigned(output, offset, offsetWidth);
        }
        output.writeBytes(values);
        return output.toByteArray();
    }

    private static byte[] shortString(String value) {
        byte[] encoded = utf8(value);
        if (encoded.length > 63) {
            throw new IllegalArgumentException("Not a short string");
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write((encoded.length << 2) | 1);
        output.writeBytes(encoded);
        return output.toByteArray();
    }

    private static void writeUnsigned(ByteArrayOutputStream output, long value, int width) {
        for (int index = 0; index < width; ++index) {
            output.write((int) (value >>> (index * 8)) & 0xFF);
        }
    }

    private static int minUnsignedWidth(long value) {
        if (value <= 0xFFL) {
            return 1;
        }
        if (value <= 0xFFFFL) {
            return 2;
        }
        if (value <= 0xFFFFFFL) {
            return 3;
        }
        return 4;
    }

    private static List<String> numberedKeys(int count) {
        List<String> keys = new ArrayList<>();
        for (int index = 0; index < count; ++index) {
            keys.add(String.format(Locale.ROOT, "k%03d", index));
        }
        return keys;
    }

    private static String nullObjectExpected(List<String> keys) {
        StringBuilder result = new StringBuilder("object{");
        for (int index = 0; index < keys.size(); ++index) {
            if (index != 0) {
                result.append(';');
            }
            result.append(hexUtf8(keys.get(index))).append("=null");
        }
        return result.append('}').toString();
    }

    private static String repeatedArrayExpected(int count) {
        StringBuilder result = new StringBuilder("array[");
        for (int index = 0; index < count; ++index) {
            if (index != 0) {
                result.append(';');
            }
            result.append("null");
        }
        return result.append(']').toString();
    }

    private static byte[] concat(byte[] left, byte[] right) {
        byte[] result = Arrays.copyOf(left, left.length + right.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hexUtf8(String value) {
        return HEX.formatHex(utf8(value));
    }

    private static void knownIncompatibilityCheck() {
        String bmp = "\uE000";
        String supplementary = "\uD800\uDC00";
        if (Integer.signum(bmp.compareTo(supplementary)) != 1
                || Integer.signum(UNSIGNED_UTF8.compare(bmp, supplementary)) != -1) {
            throw new IllegalStateException("Unicode ordering counterexample no longer reproduces");
        }
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder object = builder.startObject();
        object.appendKey(bmp);
        object.appendInt(1);
        object.appendKey(supplementary);
        object.appendInt(2);
        builder.endObject();
        Variant variant = builder.build();
        String metadataHex = HEX.formatHex(bytes(variant.getMetadataBuffer()));
        String valueHex = HEX.formatHex(bytes(variant.getValueBuffer()));
        if (!metadataHex.equals("0102000307ee8080f0908080")
                || !valueHex.equals("0202010000050a14020000001401000000")
                || variant.getFieldAtIndex(0).key.codePointAt(0) != 0x10000
                || variant.getFieldAtIndex(1).key.codePointAt(0) != 0xE000
                || variant.getFieldByKey(bmp).getInt() != 1
                || variant.getFieldByKey(supplementary).getInt() != 2) {
            throw new IllegalStateException("parquet-java Unicode deviation evidence changed");
        }
    }

    private static void verifyRealFourByteBoundaries() {
        String hugeKey = "k".repeat(0x1000000);
        VariantBuilder objectBuilder = new VariantBuilder();
        VariantObjectBuilder object = objectBuilder.startObject();
        object.appendKey(hugeKey);
        object.appendNull();
        objectBuilder.endObject();
        Variant objectVariant = objectBuilder.build();
        int metadataWidth = ((objectVariant.getMetadataBuffer().get(0) >>> 6) & 0x03) + 1;
        if (metadataWidth != 4 || objectVariant.getFieldByKey(hugeKey) == null) {
            throw new IllegalStateException("Real metadata 3-to-4-byte boundary failed");
        }

        byte[] payload = new byte[0x1000000 - 5];
        Arrays.fill(payload, (byte) 0xAB);
        VariantBuilder arrayBuilder = new VariantBuilder();
        VariantArrayBuilder array = arrayBuilder.startArray();
        array.appendBinary(ByteBuffer.wrap(payload));
        arrayBuilder.endArray();
        Variant arrayVariant = arrayBuilder.build();
        int valueHeader = (arrayVariant.getValueBuffer().get(0) & 0xFF) >>> 2;
        int offsetWidth = (valueHeader & 0x03) + 1;
        ByteBuffer decoded = arrayVariant.getElementAtIndex(0).getBinary();
        if (offsetWidth != 4
                || decoded.remaining() != payload.length
                || !Arrays.equals(payload, bytes(decoded))) {
            throw new IllegalStateException("Real value-offset 3-to-4-byte boundary failed");
        }
    }
}

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
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.THiveSerDeProperties;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Pure, metastore-parameter-only helpers ported verbatim from the legacy fe-core write planner
 * ({@code planner.BaseExternalTableDataSink} format/compress resolution and
 * {@code datasource.hive.HiveProperties} + {@code HiveMetaStoreClientHelper} SerDe-delimiter resolution),
 * homed here so {@link HiveWritePlanProvider} stays readable. Operates only on the SPI
 * {@link HmsTableInfo} (table params + SD params + serialization lib) and Thrift types — no fe-core.
 */
final class HiveSinkHelper {

    private HiveSinkHelper() {
    }

    // The write file-format types the hive sink supports (legacy HiveTableSink.supportedTypes). FORMAT_UNKNOWN
    // is intentionally absent so an unrecognized input format is rejected loudly.
    private static final Set<TFileFormatType> SUPPORTED_FORMAT_TYPES = EnumSet.of(
            TFileFormatType.FORMAT_CSV_PLAIN,
            TFileFormatType.FORMAT_ORC,
            TFileFormatType.FORMAT_PARQUET,
            TFileFormatType.FORMAT_TEXT);

    // Serialization lib that triggers multi-char field delimiters (legacy
    // HiveMetaStoreClientHelper.HIVE_MULTI_DELIMIT_SERDE — NOT the contrib MultiDelimitSerDe variant).
    private static final String HIVE_MULTI_DELIMIT_SERDE = "org.apache.hadoop.hive.serde2.MultiDelimitSerDe";

    // SerDe / SD property keys (legacy HiveProperties).
    private static final String PROP_FIELD_DELIMITER = "field.delim";
    private static final String PROP_SERIALIZATION_FORMAT = "serialization.format";
    private static final String PROP_LINE_DELIMITER = "line.delim";
    // "colelction.delim" is Hive2's intentional typo; "collection.delim" is Hive3. Both are checked.
    private static final String PROP_COLLECTION_DELIMITER_HIVE2 = "colelction.delim";
    private static final String PROP_COLLECTION_DELIMITER_HIVE3 = "collection.delim";
    private static final String PROP_MAP_KV_DELIMITER = "mapkey.delim";
    private static final String PROP_ESCAPE_DELIMITER = "escape.delim";
    private static final String PROP_NULL_FORMAT = "serialization.null.format";

    // Per-delimiter defaults (legacy HiveProperties).
    private static final String DEFAULT_FIELD_DELIMITER = "\1";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_COLLECTION_DELIMITER = "\2";
    private static final String DEFAULT_MAP_KV_DELIMITER = "\003";
    private static final String DEFAULT_ESCAPE_DELIMITER = "\\";
    private static final String DEFAULT_NULL_FORMAT = "\\N";

    /**
     * Port of legacy {@code BaseExternalTableDataSink.getTFileFormatType}. LZO input formats are rejected
     * FIRST — their class names also contain "text" (e.g. {@code LzoTextInputFormat}) and would otherwise
     * match {@code FORMAT_CSV_PLAIN}, but the BE hive sink has no LZO codec so a Doris-written LZO partition
     * becomes permanently invisible. Then {@code orc}/{@code parquet}/{@code text} substring-match (text maps
     * to {@code FORMAT_CSV_PLAIN}); an unsupported result is rejected loudly. Called for the table SD and for
     * every existing partition SD (per-partition LZO guard).
     */
    static TFileFormatType getTFileFormatType(String format) {
        if (format != null && format.toLowerCase(Locale.ROOT).contains("lzo")) {
            throw new DorisConnectorException("INSERT INTO is not supported for LZO Hive tables "
                    + "(input format: " + format + "). LZO tables are read-only in Doris.");
        }
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_UNKNOWN;
        String lowerCase = format.toLowerCase(Locale.ROOT);
        if (lowerCase.contains("orc")) {
            fileFormatType = TFileFormatType.FORMAT_ORC;
        } else if (lowerCase.contains("parquet")) {
            fileFormatType = TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCase.contains("text")) {
            fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        }
        if (!SUPPORTED_FORMAT_TYPES.contains(fileFormatType)) {
            throw new DorisConnectorException("Unsupported input format type: " + format);
        }
        return fileFormatType;
    }

    /** Port of legacy {@code BaseExternalTableDataSink.getTFileCompressType} (hive codecs; null -&gt; PLAIN). */
    static TFileCompressType getTFileCompressType(String compressType) {
        if ("snappy".equalsIgnoreCase(compressType)) {
            return TFileCompressType.SNAPPYBLOCK;
        } else if ("lz4".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZ4BLOCK;
        } else if ("lzo".equalsIgnoreCase(compressType)) {
            return TFileCompressType.LZO;
        } else if ("zlib".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZLIB;
        } else if ("zstd".equalsIgnoreCase(compressType)) {
            return TFileCompressType.ZSTD;
        } else if ("gzip".equalsIgnoreCase(compressType)) {
            return TFileCompressType.GZ;
        } else if ("bzip2".equalsIgnoreCase(compressType)) {
            return TFileCompressType.BZ2;
        } else if ("uncompressed".equalsIgnoreCase(compressType)) {
            return TFileCompressType.PLAIN;
        } else {
            // try to use plain type to decompress parquet or orc file
            return TFileCompressType.PLAIN;
        }
    }

    /**
     * Port of legacy {@code HiveTableSink.setSerDeProperties} + {@code HiveProperties}: builds the six BE
     * SerDe delimiters from the table's SD/table parameters. Field delimiter allows multi-char only for the
     * {@code MultiDelimitSerDe} lib; the escape char is emitted only when present.
     */
    static THiveSerDeProperties buildSerDeProperties(HmsTableInfo table) {
        Map<String, String> tableParams = table.getParameters();
        Map<String, String> sdParams = table.getSdParameters();
        String serDeLib = table.getSerializationLib();

        THiveSerDeProperties serDeProperties = new THiveSerDeProperties();
        // 1. field delimiter
        if (HIVE_MULTI_DELIMIT_SERDE.equals(serDeLib)) {
            serDeProperties.setFieldDelim(getFieldDelimiter(tableParams, sdParams, true));
        } else {
            serDeProperties.setFieldDelim(getFieldDelimiter(tableParams, sdParams, false));
        }
        // 2. line delimiter
        serDeProperties.setLineDelim(getLineDelimiter(tableParams, sdParams));
        // 3. collection delimiter
        serDeProperties.setCollectionDelim(getCollectionDelimiter(tableParams, sdParams));
        // 4. mapkv delimiter
        serDeProperties.setMapkvDelim(getMapKvDelimiter(tableParams, sdParams));
        // 5. escape delimiter (only when present)
        getEscapeDelimiter(tableParams, sdParams).ifPresent(serDeProperties::setEscapeChar);
        // 6. null format
        serDeProperties.setNullFormat(getNullFormat(tableParams, sdParams));
        return serDeProperties;
    }

    private static String getFieldDelimiter(Map<String, String> tableParams, Map<String, String> sdParams,
            boolean supportMultiChar) {
        Optional<String> fieldDelim = getSerdeProperty(tableParams, sdParams, PROP_FIELD_DELIMITER);
        Optional<String> serFormat = getSerdeProperty(tableParams, sdParams, PROP_SERIALIZATION_FORMAT);
        String delimiter = firstPresentOrDefault("", fieldDelim, serFormat);
        return supportMultiChar ? delimiter : getByte(delimiter, DEFAULT_FIELD_DELIMITER);
    }

    private static String getLineDelimiter(Map<String, String> tableParams, Map<String, String> sdParams) {
        Optional<String> lineDelim = getSerdeProperty(tableParams, sdParams, PROP_LINE_DELIMITER);
        return getByte(firstPresentOrDefault("", lineDelim), DEFAULT_LINE_DELIMITER);
    }

    private static String getMapKvDelimiter(Map<String, String> tableParams, Map<String, String> sdParams) {
        Optional<String> mapkvDelim = getSerdeProperty(tableParams, sdParams, PROP_MAP_KV_DELIMITER);
        return getByte(firstPresentOrDefault("", mapkvDelim), DEFAULT_MAP_KV_DELIMITER);
    }

    private static String getCollectionDelimiter(Map<String, String> tableParams, Map<String, String> sdParams) {
        Optional<String> collectionDelimHive2 = getSerdeProperty(tableParams, sdParams,
                PROP_COLLECTION_DELIMITER_HIVE2);
        Optional<String> collectionDelimHive3 = getSerdeProperty(tableParams, sdParams,
                PROP_COLLECTION_DELIMITER_HIVE3);
        return getByte(firstPresentOrDefault("", collectionDelimHive2, collectionDelimHive3),
                DEFAULT_COLLECTION_DELIMITER);
    }

    private static Optional<String> getEscapeDelimiter(Map<String, String> tableParams,
            Map<String, String> sdParams) {
        Optional<String> escapeDelim = getSerdeProperty(tableParams, sdParams, PROP_ESCAPE_DELIMITER);
        if (escapeDelim.isPresent()) {
            return Optional.of(getByte(escapeDelim.get(), DEFAULT_ESCAPE_DELIMITER));
        }
        return Optional.empty();
    }

    private static String getNullFormat(Map<String, String> tableParams, Map<String, String> sdParams) {
        Optional<String> nullFormat = getSerdeProperty(tableParams, sdParams, PROP_NULL_FORMAT);
        return firstPresentOrDefault(DEFAULT_NULL_FORMAT, nullFormat);
    }

    // Port of legacy HiveMetaStoreClientHelper.getSerdeProperty: table parameters win over SD SerDe parameters.
    private static Optional<String> getSerdeProperty(Map<String, String> tableParams,
            Map<String, String> sdParams, String key) {
        String valueFromTbl = tableParams == null ? null : tableParams.get(key);
        String valueFromSd = sdParams == null ? null : sdParams.get(key);
        return firstNonNullable(valueFromTbl, valueFromSd);
    }

    private static Optional<String> firstNonNullable(String first, String second) {
        if (first != null) {
            return Optional.of(first);
        }
        if (second != null) {
            return Optional.of(second);
        }
        return Optional.empty();
    }

    private static String firstPresentOrDefault(String defaultValue, Optional<String> first) {
        return first.orElse(defaultValue);
    }

    private static String firstPresentOrDefault(String defaultValue, Optional<String> first,
            Optional<String> second) {
        if (first.isPresent()) {
            return first.get();
        }
        if (second.isPresent()) {
            return second.get();
        }
        return defaultValue;
    }

    /**
     * Port of legacy {@code HiveMetaStoreClientHelper.getByte}: a numeric delimiter value like {@code "9"}
     * decodes to the byte char {@code (byte + 256) % 256}; a non-numeric value falls back to its first raw
     * char; a null/empty value falls back to {@code defValue}.
     */
    static String getByte(String altValue, String defValue) {
        if (altValue != null && altValue.length() > 0) {
            try {
                return Character.toString((char) ((Byte.parseByte(altValue) + 256) % 256));
            } catch (NumberFormatException e) {
                return altValue.substring(0, 1);
            }
        }
        return defValue;
    }
}

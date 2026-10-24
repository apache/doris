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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;

/** Statement-scoped Paimon write target shared by sink planning and transaction commit. */
final class PaimonWriteBinding {

    /**
     * Option on the table that the BE writer deserializes: whether it pads the CHAR values of the rows it inserts
     * (session variable enable_paimon_write_char_padding). Keep in sync with PaimonWriteSchema
     * .DORIS_WRITE_CHAR_PADDING in be-java-extensions/paimon-scanner.
     */
    static final String DORIS_WRITE_CHAR_PADDING = "doris.write.char-padding";

    private final String tableName;
    private final FileStoreTable table;
    private final String serializedTable;
    private final Map<String, String> hadoopConfig;
    private final boolean overwrite;
    private final Map<String, String> staticPartition;

    private PaimonWriteBinding(String tableName, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> staticPartition, boolean charPadding) {
        this.tableName = tableName;
        this.table = table;
        // The FE committer keeps the catalog-aware table, but the BE writer must not deserialize
        // an HMS/DLF catalog loader whose metastore classes are absent from the BE plugin.
        this.serializedTable = serialize(withCharPadding(PaimonScanPlanProvider.dropCatalogLoader(table),
                charPadding));
        this.hadoopConfig = Collections.unmodifiableMap(new LinkedHashMap<>(hadoopConfig));
        this.overwrite = overwrite;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
    }

    /**
     * @param charPadding whether the BE writer pads the CHAR values of the rows it inserts, and the static
     *                    partition values are padded to match them
     */
    static PaimonWriteBinding create(PaimonTableHandle handle, FileStoreTable table,
            Map<String, String> hadoopConfig, ConnectorWriteHandle writeHandle, boolean charPadding) {
        String tableName = handle.getDatabaseName() + "." + handle.getTableName();
        checkBinaryPartitionNames(tableName, table);
        boolean overwrite = writeHandle.isOverwrite();
        // Only a static overwrite reads the partition values. An INSERT writes each row to the partition its own
        // values name, so it leaves them alone: casting them could only fail a write the rows would accept.
        Map<String, String> staticPartition = overwrite
                ? resolveStaticPartition(table, writeHandle, charPadding) : Collections.emptyMap();
        FileStoreTable writeTable = configureTableForWrite(table, overwrite, staticPartition);
        return new PaimonWriteBinding(tableName, writeTable, hadoopConfig, overwrite, staticPartition, charPadding);
    }

    /**
     * With {@code partition.legacy-name} = true, the default, Paimon names a partition after the string of each
     * value, which for a binary value is the Java array object ({@code [B@1b6d3586}) rather than its bytes. Every
     * resolution of the name differs, so a write fails its commit after the BE wrote its files under one of them,
     * and Paimon itself cannot read such a partition back.
     */
    static void checkBinaryPartitionNames(String tableName, FileStoreTable table) {
        if (!CoreOptions.fromMap(table.options()).legacyPartitionName()) {
            return;
        }
        for (String partitionKey : table.partitionKeys()) {
            if (isBinary(table.rowType().getField(partitionKey).type())) {
                throw new DorisConnectorException("Cannot write Paimon table " + tableName + ": its binary"
                        + " partition column '" + partitionKey + "' needs the table option 'partition.legacy-name'"
                        + " = 'false'. With legacy partition names Paimon names a binary partition after a Java"
                        + " array object rather than its bytes and cannot read the partition back");
            }
        }
    }

    private static boolean isBinary(DataType type) {
        return type.getTypeRoot() == DataTypeRoot.BINARY || type.getTypeRoot() == DataTypeRoot.VARBINARY;
    }

    /**
     * The table the BE writer gets: the FE table with {@link #DORIS_WRITE_CHAR_PADDING} among its options. Only
     * the schema is replaced, so nothing else of the table changes, and the FE committer's table goes without it.
     */
    private static FileStoreTable withCharPadding(FileStoreTable table, boolean charPadding) {
        Map<String, String> options = new HashMap<>(table.schema().options());
        options.put(DORIS_WRITE_CHAR_PADDING, String.valueOf(charPadding));
        return table.copy(table.schema().copy(options));
    }

    static FileStoreTable configureTableForWrite(FileStoreTable table, boolean overwrite,
            Map<String, String> staticPartition) {
        if (!overwrite) {
            return table;
        }
        String dynamicOverwriteKey = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();
        boolean explicitlyDynamic = staticPartition.isEmpty()
                && Boolean.parseBoolean(table.options().get(dynamicOverwriteKey));
        if (explicitlyDynamic) {
            return table;
        }
        return table.copy(Collections.singletonMap(dynamicOverwriteKey, Boolean.FALSE.toString()));
    }

    /**
     * The static partition of an overwrite, as Paimon's static overwrite parses it: SQL NULL becomes the table's
     * {@code partition.default-name}, and every other value is the one the written rows carry, cast to the
     * column type, and padded as the BE writer pads the CHAR values of the rows ({@link #charPaddedFields}): Paimon
     * parses a partition value without padding it. A binary value is the text of its bytes ({@link
     * #binaryStaticValue}).
     */
    static Map<String, String> resolveStaticPartition(FileStoreTable table, ConnectorWriteHandle writeHandle,
            boolean charPadding) {
        Map<String, String> canonicalNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String partitionKey : table.partitionKeys()) {
            canonicalNames.put(partitionKey, partitionKey);
        }
        Map<String, String> requestedColumns = new LinkedHashMap<>();
        for (String key : writeHandle.getStaticPartitionSpec().keySet()) {
            String canonicalName = canonicalNames.get(key);
            if (canonicalName == null) {
                throw new DorisConnectorException("Column '" + key
                        + "' is not a partition column of Paimon table");
            }
            requestedColumns.put(key, canonicalName);
        }
        CoreOptions options = CoreOptions.fromMap(table.options());
        String defaultPartitionName = options.partitionDefaultName();
        Predicate<String> charPaddedFields = charPaddedFields(table.schema(), charPadding);
        Map<String, String> castValues = writeHandle.getCastStaticPartitionSpec();
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> requested : requestedColumns.entrySet()) {
            String key = requested.getKey();
            String canonicalName = requested.getValue();
            if (writeHandle.getStaticPartitionNullKeys().contains(key)) {
                result.put(canonicalName, defaultPartitionName);
                continue;
            }
            String value = Objects.requireNonNull(castValues.get(key),
                    () -> "missing the cast value of static partition column " + key);
            DataType type = table.rowType().getField(canonicalName).type();
            if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                value = toSdkLocalTime(canonicalName, value);
            }
            if (type.getTypeRoot() == DataTypeRoot.CHAR && charPaddedFields.test(canonicalName)) {
                value = padChar(value, ((CharType) type).getLength());
            }
            if (isBinary(type)) {
                value = binaryStaticValue(canonicalName, value, type);
            }
            if (defaultPartitionName.equals(value)) {
                // Paimon's static overwrite reads this string as the NULL partition of any partition type, so
                // a value equal to it would overwrite the NULL partition instead.
                throw new DorisConnectorException("Static partition value for column '" + canonicalName
                        + "' equals Paimon partition.default-name '" + defaultPartitionName
                        + "' and cannot be represented in a static overwrite");
            }
            result.put(canonicalName, value);
        }
        return result;
    }

    /**
     * The fields whose CHAR values the BE writer pads, as PaimonWriteSchema.create decides it: with {@code charPadding}
     * on, every field but the table's {@code rowkind.field}, from whose value Paimon decodes the row kind, and of a
     * table that writes ORC files only the partition fields. ORC pads every CHAR value it writes and strips the
     * trailing spaces from one it reads, so the writer leaves the other fields of such a table alone. Keep in sync
     * with PaimonWriteSchema.create and PaimonWriteSchema.writesOrcFiles in be-java-extensions/paimon-scanner.
     */
    static Predicate<String> charPaddedFields(TableSchema schema, boolean charPadding) {
        if (!charPadding) {
            return field -> false;
        }
        CoreOptions options = CoreOptions.fromMap(schema.options());
        String rowKindField = options.rowkindField().orElse(null);
        boolean orcFiles = CoreOptions.FILE_FORMAT_ORC.equals(options.formatType())
                || options.fileFormatPerLevel().containsValue(CoreOptions.FILE_FORMAT_ORC);
        List<String> partitionKeys = schema.partitionKeys();
        return field -> !field.equals(rowKindField) && (!orcFiles || partitionKeys.contains(field));
    }

    /**
     * Paimon parses a static overwrite value of a TIMESTAMP WITH LOCAL TIME ZONE column as local time in the FE
     * JVM's default zone. Doris binds such a column as TIMESTAMPTZ, so the cast value is the instant in UTC with
     * its offset; it is moved to the JVM zone, with a space between the date and the time as Paimon's parser
     * needs. An instant whose local time a DST change repeats there is rejected: both instants of the overlap
     * format to the same value, which Paimon reads as the earlier one.
     */
    private static String toSdkLocalTime(String column, String value) {
        ZoneId sdkZone = ZoneId.systemDefault();
        LocalDateTime sdkLocal = OffsetDateTime.parse(value.replace(' ', 'T'), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .atZoneSameInstant(sdkZone).toLocalDateTime();
        if (sdkZone.getRules().getValidOffsets(sdkLocal).size() > 1) {
            throw new DorisConnectorException("Static LTZ partition value for column '" + column
                    + "' is ambiguous in FE JVM time zone " + sdkZone
                    + " and cannot be represented in a static overwrite");
        }
        return sdkLocal.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace('T', ' ');
    }

    /**
     * Paimon's static overwrite takes the UTF-8 bytes of a partition value's string, so the bytes the rows carry,
     * which reach it as {@code 0x} and their hex digits, name their partition only as the text they decode to. A
     * wrong name would overwrite another partition without an error, so bytes that are not UTF-8 are rejected;
     * strict decoding also rejects overlong and surrogate sequences, so the text encodes back to the same bytes.
     * Paimon keeps those bytes for a BINARY column whatever their length, as the rows keep them, and fails the
     * commit for a VARBINARY value longer than its column, so such a value is rejected here, before any file is
     * written.
     */
    private static String binaryStaticValue(String column, String value, DataType type) {
        if (!value.startsWith("0x")) {
            throw new IllegalStateException("Binary static partition value of column " + column
                    + " lacks the 0x prefix: " + value);
        }
        byte[] bytes = HexFormat.of().parseHex(value.substring(2));
        if (type.getTypeRoot() == DataTypeRoot.VARBINARY && bytes.length > ((VarBinaryType) type).getLength()) {
            throw new DorisConnectorException("Static partition value " + value + " of binary column '" + column
                    + "' has " + bytes.length + " bytes, more than its type " + type + " holds, which a static"
                    + " overwrite of a Paimon table rejects");
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new DorisConnectorException("Static partition value " + value + " of binary column '" + column
                    + "' is not UTF-8 text, which a static overwrite of a Paimon table needs to name its partition");
        }
    }

    /**
     * Pads a CHAR value with spaces to the column length in code points, as PaimonWriteSchema pads the rows' CHAR
     * values; a longer value is kept, as it is there.
     */
    private static String padChar(String value, int length) {
        int padLength = length - value.codePointCount(0, value.length());
        return padLength > 0 ? value + " ".repeat(padLength) : value;
    }

    private static String serialize(FileStoreTable table) {
        try {
            return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(table));
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to serialize Paimon write table", e);
        }
    }

    String tableName() {
        return tableName;
    }

    FileStoreTable getTable() {
        return table;
    }

    String getSerializedTable() {
        return serializedTable;
    }

    Map<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    Map<String, String> getStaticPartition() {
        return staticPartition;
    }
}

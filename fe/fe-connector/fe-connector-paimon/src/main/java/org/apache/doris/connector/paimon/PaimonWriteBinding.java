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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
        boolean overwrite = writeHandle.isOverwrite();
        // Only a static overwrite reads the partition values. An INSERT writes each row to the partition its own
        // values name, so it leaves them alone: casting them could only fail a write the rows would accept.
        Map<String, String> staticPartition = overwrite
                ? resolveStaticPartition(table, writeHandle, charPadding) : Collections.emptyMap();
        FileStoreTable writeTable = configureTableForWrite(table, overwrite, staticPartition);
        return new PaimonWriteBinding(handle.getDatabaseName() + "." + handle.getTableName(),
                writeTable, hadoopConfig, overwrite, staticPartition, charPadding);
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
     * column type, and padded as the BE writer pads the rows' CHAR values when {@code charPadding} is on: Paimon
     * parses a partition value without padding it. A binary column can only be statically overwritten with NULL.
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
            DataTypeRoot typeRoot = table.rowType().getField(canonicalName).type().getTypeRoot();
            if ((typeRoot == DataTypeRoot.BINARY || typeRoot == DataTypeRoot.VARBINARY)
                    && !writeHandle.getStaticPartitionNullKeys().contains(key)) {
                // Paimon's static overwrite takes the UTF-8 bytes of a value's string, which cannot name every
                // binary partition, and a wrong name overwrites another partition without an error. Checked
                // before the values are cast.
                throw new DorisConnectorException("Static overwrite of binary partition column '"
                        + canonicalName + "' is not supported");
            }
            requestedColumns.put(key, canonicalName);
        }
        String defaultPartitionName = CoreOptions.fromMap(table.options()).partitionDefaultName();
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
            if (charPadding && type.getTypeRoot() == DataTypeRoot.CHAR) {
                value = padChar(value, ((CharType) type).getLength());
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

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

package org.apache.doris.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.DefaultValueRow;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.BinaryStringUtils;
import org.apache.paimon.utils.DefaultValueUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable mapping from Doris input columns to a Paimon table row.
 *
 * <p>The input may contain a subset of table columns in a different order. This class
 * resolves their types and table positions once, then converts each input row to the
 * table-schema layout expected by the Paimon writer.
 */
final class PaimonWriteSchema {
    static final String ROW_KIND_COLUMN = "__DORIS_PAIMON_ROW_KIND__";
    static final byte INSERT_OPERATION = 0;
    static final byte UPDATE_OPERATION = 1;
    static final byte DELETE_OPERATION = 2;
    /**
     * Table option through which FE asks the writer to pad CHAR values, set from the session variable
     * enable_paimon_write_char_padding. Keep in sync with PaimonWriteBinding.DORIS_WRITE_CHAR_PADDING in
     * fe-connector-paimon.
     */
    static final String DORIS_WRITE_CHAR_PADDING = "doris.write.char-padding";

    private final InternalRow.FieldGetter[] fieldGetters;
    /**
     * Per input column, pads the CHAR values it holds; null when it holds none, when padding is off, for the
     * table's rowkind.field, or for a column other than a partition column of a table that writes ORC files.
     */
    private final CharPadder[] charPadders;
    /**
     * Per input column, the length of a padded CHAR primary-key column of a changelog write, or 0. Padding a
     * shorter key that MERGE inserts would change the key after MERGE matched it and checked it for duplicates;
     * see {@link #padRow}.
     */
    private final int[] insertedKeyCharLengths;
    /** Decodes the kind of a row from the table's rowkind.field as Paimon does when it writes the row, or null. */
    private final RowKindGenerator rowKindGenerator;
    /**
     * Fills a NULL field with its default value, as Paimon does before it decodes the row kind; null when the table
     * has no default value.
     */
    private final DefaultValueRow defaultValueRow;
    /** Whether the write overwrites the partitions it writes; see {@link #padsRow}. */
    private final boolean overwrite;
    private final RowType inputType;
    /** Maps Doris input-column position → Paimon table-schema position. */
    private final int[] tableFieldIndexes;
    /** Paimon defaults for table fields omitted from the Doris input. */
    private final int[] omittedDefaultFieldIndexes;
    private final Object[] omittedDefaultValues;
    private final int tableFieldCount;

    private PaimonWriteSchema(int[] tableFieldIndexes, int[] omittedDefaultFieldIndexes,
            Object[] omittedDefaultValues, int tableFieldCount, RowType inputType, CharPadder[] charPadders,
            int[] insertedKeyCharLengths, RowKindGenerator rowKindGenerator, DefaultValueRow defaultValueRow,
            boolean overwrite) {
        this.fieldGetters = new InternalRow.FieldGetter[inputType.getFieldCount()];
        for (int i = 0; i < fieldGetters.length; i++) {
            // Getters describe the transport row, not the target table. Transport fields are
            // nullable so an invalid Doris NULL survives projection and reaches Paimon's
            // authoritative table-schema validation.
            this.fieldGetters[i] = InternalRow.createFieldGetter(inputType.getTypeAt(i), i);
        }
        this.charPadders = charPadders;
        this.insertedKeyCharLengths = insertedKeyCharLengths;
        this.rowKindGenerator = rowKindGenerator;
        this.defaultValueRow = defaultValueRow;
        this.overwrite = overwrite;
        this.inputType = inputType;
        this.tableFieldIndexes = tableFieldIndexes;
        this.omittedDefaultFieldIndexes = omittedDefaultFieldIndexes;
        this.omittedDefaultValues = omittedDefaultValues;
        this.tableFieldCount = tableFieldCount;
    }

    /**
     * Create the write schema by resolving {@code columnNames} against the
     * Paimon table schema.
     *
     * @param tableType   full Paimon table row type (all columns in table order)
     * @param columnNames output column names from BE (in Doris output order)
     * @return immutable schema metadata for this writer session
     * @throws IllegalArgumentException if any column name is not found in the table schema
     */
    static PaimonWriteSchema create(RowType tableType, String[] columnNames) {
        return create(tableType, columnNames, false);
    }

    static PaimonWriteSchema create(
            RowType tableType, String[] columnNames, boolean changelogWrite) {
        return create(tableType, columnNames, changelogWrite, false);
    }

    static PaimonWriteSchema create(
            RowType tableType, String[] columnNames, boolean changelogWrite, boolean charPadding) {
        return create(tableType, columnNames, changelogWrite, charPadding, null);
    }

    static PaimonWriteSchema create(RowType tableType, String[] columnNames, boolean changelogWrite,
            boolean charPadding, String rowKindField) {
        return create(tableType, columnNames, changelogWrite, charPadding, rowKindField, Collections.emptyList());
    }

    static PaimonWriteSchema create(RowType tableType, String[] columnNames, boolean changelogWrite,
            boolean charPadding, String rowKindField, List<String> primaryKeys) {
        return create(tableType, columnNames, changelogWrite, charPadding, rowKindField, primaryKeys,
                Collections.emptyList(), false, false);
    }

    /**
     * @param charPadding   whether to pad the CHAR values of inserted rows as Paimon's cast to CHAR does; see
     *                      {@link #charPadder} and {@link #padsRow}
     * @param rowKindField  the table's {@code rowkind.field}, or null. Its value is not padded: Paimon decodes the row
     *                      kind from it ({@code RowKind.fromShortString}), and a padded '+I ' is no row kind.
     * @param primaryKeys   the table's primary keys. In a changelog write, a CHAR key that padding would change
     *                      fails the row; see {@link #padRow}.
     * @param partitionKeys the table's partition keys
     * @param orcFiles      whether the table writes ORC files ({@link #writesOrcFiles}). ORC pads every CHAR value
     *                      it writes and strips the trailing spaces from one it reads, so padding changes nothing
     *                      ORC stores, only the bucket of a CHAR key, which an UPDATE or DELETE that reads the key
     *                      back unpadded would then miss. Only the partition columns of such a table are padded: a
     *                      partition value is read back as it is stored.
     * @param overwrite     whether the write overwrites the partitions it writes; see {@link #padsRow}
     */
    static PaimonWriteSchema create(RowType tableType, String[] columnNames, boolean changelogWrite,
            boolean charPadding, String rowKindField, List<String> primaryKeys, List<String> partitionKeys,
            boolean orcFiles, boolean overwrite) {
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException(
                    "PaimonJniWriter requires explicit column names");
        }

        int[] tableFieldIndexes = new int[columnNames.length];
        CharPadder[] charPadders = new CharPadder[columnNames.length];
        int[] insertedKeyCharLengths = new int[columnNames.length];
        List<DataField> inputFields = new ArrayList<>(columnNames.length);
        boolean[] specifiedFields = new boolean[tableType.getFieldCount()];
        for (int i = 0; i < columnNames.length; i++) {
            if (changelogWrite && i == 0) {
                if (!ROW_KIND_COLUMN.equals(columnNames[i])) {
                    throw new IllegalArgumentException(
                            "Paimon changelog write requires row kind as the first column");
                }
                tableFieldIndexes[i] = -1;
                inputFields.add(new DataField(
                        Integer.MIN_VALUE, ROW_KIND_COLUMN, new TinyIntType(true)));
                continue;
            }
            int tableIndex = tableType.getFieldIndex(columnNames[i]);
            if (tableIndex < 0) {
                throw new IllegalArgumentException(
                        "Paimon column '" + columnNames[i] + "' not found in table schema");
            }
            if (specifiedFields[tableIndex]) {
                throw new IllegalArgumentException(
                        "Duplicate Paimon write column '" + columnNames[i] + "'");
            }
            specifiedFields[tableIndex] = true;
            DataField field = tableType.getFields().get(tableIndex);
            tableFieldIndexes[i] = tableIndex;
            // Input nullability is independent from the target table constraint. Doris can still
            // produce an explicit NULL for a NOT NULL target; carrying it through Arrow lets the
            // Paimon writer return its normal constraint diagnostic instead of reading a default
            // primitive value from a non-null getter.
            inputFields.add(field.newType(field.type().nullable()));
            if (charPadding && !field.name().equals(rowKindField)
                    && (!orcFiles || partitionKeys.contains(field.name()))) {
                charPadders[i] = charPadder(field.type(), field.name());
                if (changelogWrite && field.type().getTypeRoot() == DataTypeRoot.CHAR
                        && primaryKeys.contains(field.name())) {
                    insertedKeyCharLengths[i] = ((CharType) field.type()).getLength();
                }
            }
        }

        int[] omittedDefaultFieldIndexes = new int[tableType.getFieldCount()];
        Object[] omittedDefaultValues = new Object[tableType.getFieldCount()];
        int omittedDefaultCount = 0;
        for (int tableIndex = 0; tableIndex < tableType.getFieldCount(); tableIndex++) {
            DataField field = tableType.getFields().get(tableIndex);
            if (specifiedFields[tableIndex] || field.defaultValue() == null) {
                continue;
            }
            omittedDefaultFieldIndexes[omittedDefaultCount] = tableIndex;
            omittedDefaultValues[omittedDefaultCount] =
                    DefaultValueUtils.convertDefaultValue(field.type(), field.defaultValue());
            omittedDefaultCount++;
        }

        return new PaimonWriteSchema(
                tableFieldIndexes,
                Arrays.copyOf(omittedDefaultFieldIndexes, omittedDefaultCount),
                Arrays.copyOf(omittedDefaultValues, omittedDefaultCount),
                tableType.getFieldCount(),
                new RowType(inputFields),
                charPadders,
                insertedKeyCharLengths,
                rowKindField == null ? null : new RowKindGenerator(rowKindField, tableType),
                DefaultValueRow.create(tableType),
                overwrite);
    }

    /**
     * Whether FE asked for CHAR padding through {@link #DORIS_WRITE_CHAR_PADDING}. An FE that predates the option
     * does not set it, and does not pad its static partition values either.
     */
    static boolean charPadding(Map<String, String> tableOptions) {
        return Boolean.parseBoolean(tableOptions.get(DORIS_WRITE_CHAR_PADDING));
    }

    /** Whether the table writes ORC files, by {@code file.format} or at some level by {@code file.format.per.level}. */
    static boolean writesOrcFiles(Map<String, String> tableOptions) {
        CoreOptions options = CoreOptions.fromMap(tableOptions);
        return CoreOptions.FILE_FORMAT_ORC.equals(options.formatType())
                || options.fileFormatPerLevel().containsValue(CoreOptions.FILE_FORMAT_ORC);
    }

    /** Paimon input fields in the exact order transported by Arrow C Data. */
    RowType inputType() {
        return inputType;
    }

    /** Expand one input row to the full Paimon table-schema layout. */
    GenericRow tableRow(InternalRow columnValues) {
        if (columnValues.getFieldCount() != tableFieldIndexes.length) {
            throw new IllegalArgumentException(
                    "Paimon input value count does not match write schema");
        }
        GenericRow row = new GenericRow(tableFieldCount);
        for (int i = 0; i < omittedDefaultFieldIndexes.length; i++) {
            row.setField(omittedDefaultFieldIndexes[i], omittedDefaultValues[i]);
        }
        for (int i = 0; i < tableFieldIndexes.length; i++) {
            Object value = fieldGetters[i].getFieldOrNull(columnValues);
            if (tableFieldIndexes[i] < 0) {
                row.setRowKind(toRowKind(value));
                continue;
            }
            // Actual Doris input is applied last so an explicit NULL remains distinct
            // from an omitted field and retains Paimon's writer-side semantics.
            row.setField(tableFieldIndexes[i], value);
        }
        if (padsRow(row)) {
            padRow(row);
        }
        return row;
    }

    /**
     * Whether to pad {@code row}: whether it inserts, outside an overwrite. Its row kind, if any, is the first input
     * column. An UPDATE or DELETE row carries the key Doris read back from the table, which has to stay byte-equal
     * to the stored key for Paimon to match it, padded or not. A table with a rowkind.field takes the kind of a row
     * from that field's value when Paimon writes it ({@code TableWriteImpl#writeAndReturn}), after filling a NULL
     * with the field's default and ignoring case: '+I', '+i', or NULL under DEFAULT '+I' inserts, while a '-D', '-U'
     * or '+U' row carries a key to match, as an UPDATE or DELETE row does. A value Paimon cannot decode, such as NULL
     * without a default, fails the write in Paimon with the same error, after Paimon's own NOT NULL check, so such a
     * row is left as it is. An overwrite replaces the partitions it writes, so none of its rows matches a stored
     * key, and all of them are padded, like the static partition value FE pads into the overwrite filter: a '+U'
     * row of INSERT OVERWRITE ... PARTITION (p = 'x') has to land in 'x  ', and a '+I' and a '-D' of one key in the
     * statement still cancel out.
     */
    private boolean padsRow(GenericRow row) {
        if (row.getRowKind() != RowKind.INSERT) {
            return false;
        }
        if (overwrite || rowKindGenerator == null) {
            return true;
        }
        try {
            return rowKindGenerator.generate(defaultValueRow == null ? row : defaultValueRow.replaceRow(row))
                    == RowKind.INSERT;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void padRow(GenericRow row) {
        for (int i = 0; i < charPadders.length; i++) {
            Object value = charPadders[i] == null ? null : row.getField(tableFieldIndexes[i]);
            if (value == null) {
                continue;
            }
            // In a changelog write only MERGE inserts rows, and MERGE has matched them to the table and checked them
            // for duplicate keys by the key as it is: Doris compares CHAR values unpadded. Padding a shorter key
            // here would make 'k' and 'k ' one key, or let a NOT MATCHED row replace the stored 'k  ' it did not
            // match. A key as long as the column is not changed by padding.
            if (insertedKeyCharLengths[i] > 0 && ((BinaryString) value).numChars() < insertedKeyCharLengths[i]) {
                throw new IllegalArgumentException(String.format(
                        "MERGE cannot insert '%s' into the CHAR(%d) primary key column '%s' of a Paimon table: "
                                + "the value is shorter than the column, and padding it to the column "
                                + "length would change the key that MERGE matched. Pad the key in the "
                                + "MERGE, with rpad(<key>, %d, ' ') in both the ON condition and the "
                                + "inserted values, or set enable_paimon_write_char_padding = false",
                        value, insertedKeyCharLengths[i], inputType.getFields().get(i).name(),
                        insertedKeyCharLengths[i]));
            }
            row.setField(tableFieldIndexes[i], charPadders[i].pad(value));
        }
    }

    /**
     * Returns the padder of a value of {@code type} in {@code column}, or null when the type holds no CHAR. A CHAR
     * value is padded with spaces to the type length in code points, as Paimon's cast to CHAR pads it
     * ({@code BinaryStringUtils.toCharacterString}), and so are the CHAR values nested in an ARRAY, a MAP or a
     * ROW. Unlike that cast, a longer value is kept: Doris has already cut it to the length or kept it, as the
     * session asks. A map whose keys padding would make equal fails the row; see {@link #padMapKeys}.
     */
    private static CharPadder charPadder(DataType type, String column) {
        switch (type.getTypeRoot()) {
            case CHAR: {
                int length = ((CharType) type).getLength();
                return value -> padChar((BinaryString) value, length);
            }
            case ARRAY: {
                DataType elementType = ((ArrayType) type).getElementType();
                CharPadder elementPadder = charPadder(elementType, column);
                if (elementPadder == null) {
                    return null;
                }
                InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType.nullable());
                return value -> padArray((InternalArray) value, elementGetter, elementPadder);
            }
            case MAP: {
                MapType mapType = (MapType) type;
                CharPadder keyPadder = charPadder(mapType.getKeyType(), column);
                CharPadder valuePadder = charPadder(mapType.getValueType(), column);
                if (keyPadder == null && valuePadder == null) {
                    return null;
                }
                InternalArray.ElementGetter keyGetter =
                        InternalArray.createElementGetter(mapType.getKeyType().nullable());
                InternalArray.ElementGetter valueGetter =
                        InternalArray.createElementGetter(mapType.getValueType().nullable());
                MapKeyFormat keyFormat = keyPadder == null ? null : new MapKeyFormat(mapType.getKeyType());
                return value -> {
                    InternalMap map = (InternalMap) value;
                    return new PaddedMap(
                            keyPadder == null ? map.keyArray() : padMapKeys(map.keyArray(), keyGetter, keyPadder,
                                    keyFormat, column),
                            valuePadder == null
                                    ? map.valueArray() : padArray(map.valueArray(), valueGetter, valuePadder));
                };
            }
            case ROW: {
                List<DataField> fields = ((RowType) type).getFields();
                CharPadder[] fieldPadders = new CharPadder[fields.size()];
                InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fields.size()];
                boolean holdsChar = false;
                for (int i = 0; i < fields.size(); i++) {
                    fieldPadders[i] = charPadder(fields.get(i).type(), column);
                    fieldGetters[i] = InternalRow.createFieldGetter(fields.get(i).type().nullable(), i);
                    holdsChar |= fieldPadders[i] != null;
                }
                if (!holdsChar) {
                    return null;
                }
                return value -> {
                    InternalRow nested = (InternalRow) value;
                    GenericRow padded = new GenericRow(fieldGetters.length);
                    for (int i = 0; i < fieldGetters.length; i++) {
                        Object field = fieldGetters[i].getFieldOrNull(nested);
                        padded.setField(i, field == null || fieldPadders[i] == null
                                ? field : fieldPadders[i].pad(field));
                    }
                    return padded;
                };
            }
            default:
                return null;
        }
    }

    private static BinaryString padChar(BinaryString value, int length) {
        int padLength = length - value.numChars();
        return padLength > 0 ? BinaryStringUtils.concat(value, BinaryString.blankString(padLength)) : value;
    }

    private static InternalArray padArray(InternalArray array, InternalArray.ElementGetter elementGetter,
            CharPadder elementPadder) {
        Object[] elements = new Object[array.size()];
        for (int i = 0; i < elements.length; i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            elements[i] = element == null ? null : elementPadder.pad(element);
        }
        return new GenericArray(elements);
    }

    /**
     * Pads the keys of one map, failing when padding makes two of its distinct keys one: a map holds one entry per
     * key, and Paimon's Avro reader, for one, keeps only one of them. Keys equal before padding are kept as they are.
     * Keys are compared and printed through {@code keyFormat}, never by their own hashCode, equals or toString: a key
     * of a nested type holds the columnar rows and arrays of the Arrow batch, which support none of them
     * (ColumnarArray keeps identity equality), so hashing the key of a MAP<ROW<c CHAR(2), r ROW<i INT>>, INT> itself
     * would fail even a map of one entry.
     */
    private static InternalArray padMapKeys(InternalArray keys, InternalArray.ElementGetter keyGetter,
            CharPadder keyPadder, MapKeyFormat keyFormat, String column) {
        Object[] padded = new Object[keys.size()];
        // Bytes of a padded key -> position of the first key padded into it.
        Map<BinaryRow, Integer> firstKeyOfPadded = new HashMap<>();
        for (int i = 0; i < padded.length; i++) {
            Object key = keyGetter.getElementOrNull(keys, i);
            padded[i] = key == null ? null : keyPadder.pad(key);
            Integer first = firstKeyOfPadded.putIfAbsent(keyFormat.bytes(padded[i]), i);
            if (first == null) {
                continue;
            }
            Object firstKey = keyGetter.getElementOrNull(keys, first);
            if (!keyFormat.bytes(firstKey).equals(keyFormat.bytes(key))) {
                throw new IllegalArgumentException(String.format(
                        "Padding the CHAR keys of a map in column '%s' of a Paimon table to the CHAR length turns "
                                + "the keys '%s' and '%s' into the same key '%s', and a map holds one entry per key. "
                                + "Make the keys distinct, or set enable_paimon_write_char_padding = false",
                        column, keyFormat.text(firstKey), keyFormat.text(key), keyFormat.text(padded[i])));
            }
        }
        return new GenericArray(padded);
    }

    /** Compares the keys of a map by their bytes and prints them as Paimon casts them to STRING, for any key type. */
    private static final class MapKeyFormat {
        private final InternalRowSerializer serializer;
        private final CastExecutor<Object, Object> toText;

        @SuppressWarnings("unchecked")
        private MapKeyFormat(DataType keyType) {
            this.serializer = new InternalRowSerializer(keyType);
            this.toText = (CastExecutor<Object, Object>) CastExecutors.resolveToString(keyType);
        }

        /** The bytes of {@code key} as the only field of a row, copied since the serializer reuses its buffer. */
        private BinaryRow bytes(Object key) {
            return serializer.toBinaryRow(GenericRow.of(key)).copy();
        }

        /** {@code key} as Paimon casts it to STRING: a ROW key (k, 1) prints as {k, 1}. */
        private String text(Object key) {
            return String.valueOf(toText.cast(key));
        }
    }

    /** Pads the CHAR values held by one non-null value of an input column. */
    private interface CharPadder {
        Object pad(Object value);
    }

    /** A map over padded key and value arrays, which keeps the entries of the input map in their order. */
    private static final class PaddedMap implements InternalMap {
        private final InternalArray keys;
        private final InternalArray values;

        private PaddedMap(InternalArray keys, InternalArray values) {
            this.keys = keys;
            this.values = values;
        }

        @Override
        public int size() {
            return keys.size();
        }

        @Override
        public InternalArray keyArray() {
            return keys;
        }

        @Override
        public InternalArray valueArray() {
            return values;
        }
    }

    private static RowKind toRowKind(Object operation) {
        if (!(operation instanceof Byte)) {
            throw new IllegalArgumentException("Paimon row change operation must be a TINYINT");
        }
        switch ((Byte) operation) {
            case INSERT_OPERATION:
                return RowKind.INSERT;
            case UPDATE_OPERATION:
                return RowKind.UPDATE_AFTER;
            case DELETE_OPERATION:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Unknown Paimon row change operation: " + operation);
        }
    }
}

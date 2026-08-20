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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.DefaultValueUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    private final InternalRow.FieldGetter[] fieldGetters;
    private final RowType inputType;
    /** Maps Doris input-column position → Paimon table-schema position. */
    private final int[] tableFieldIndexes;
    /** Paimon defaults for table fields omitted from the Doris input. */
    private final int[] omittedDefaultFieldIndexes;
    private final Object[] omittedDefaultValues;
    private final int tableFieldCount;

    private PaimonWriteSchema(DataType[] targetTypes, int[] tableFieldIndexes,
            int[] omittedDefaultFieldIndexes, Object[] omittedDefaultValues, int tableFieldCount,
            RowType inputType) {
        this.fieldGetters = new InternalRow.FieldGetter[targetTypes.length];
        for (int i = 0; i < targetTypes.length; i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(targetTypes[i], i);
        }
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
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException(
                    "PaimonJniWriter requires explicit column names");
        }

        DataType[] targetTypes = new DataType[columnNames.length];
        int[] tableFieldIndexes = new int[columnNames.length];
        List<DataField> inputFields = new ArrayList<>(columnNames.length);
        boolean[] specifiedFields = new boolean[tableType.getFieldCount()];
        for (int i = 0; i < columnNames.length; i++) {
            if (changelogWrite && i == 0) {
                if (!ROW_KIND_COLUMN.equals(columnNames[i])) {
                    throw new IllegalArgumentException(
                            "Paimon changelog write requires row kind as the first column");
                }
                targetTypes[i] = new TinyIntType(false);
                tableFieldIndexes[i] = -1;
                inputFields.add(new DataField(
                        Integer.MIN_VALUE, ROW_KIND_COLUMN, targetTypes[i]));
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
            targetTypes[i] = field.type();
            tableFieldIndexes[i] = tableIndex;
            inputFields.add(field);
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
                targetTypes,
                tableFieldIndexes,
                Arrays.copyOf(omittedDefaultFieldIndexes, omittedDefaultCount),
                Arrays.copyOf(omittedDefaultValues, omittedDefaultCount),
                tableType.getFieldCount(),
                new RowType(inputFields));
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
        return row;
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

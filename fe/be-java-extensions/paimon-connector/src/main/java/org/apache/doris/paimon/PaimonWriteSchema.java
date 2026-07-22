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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DefaultValueUtils;

/**
 * Immutable input-column metadata shared by all batches of one writer.
 *
 * <p>Doris may produce a subset or permutation of the Paimon table columns. The
 * SDK {@code write(row)} API, including its partition and bucket extractors,
 * expects rows in table-schema order. This class resolves the input columns once
 * and expands every Arrow row to that canonical layout.
 *
 * <h3>Partial-write support</h3>
 * Missing columns with schema defaults are materialized before the row reaches
 * Paimon's nullability check; other missing columns remain null. Reordered columns
 * are placed at their table-schema positions, so routing and file writing always
 * observe the same layout.
 */
final class PaimonWriteSchema {
    private final DataType[] targetTypes;
    /** Maps Doris input-column position → Paimon table-schema position. */
    private final int[] tableFieldIndexes;
    /** Default values for omitted columns, in Paimon table-schema order. */
    private final Object[] omittedFieldValues;
    private final int tableFieldCount;
    private final boolean partial;

    private PaimonWriteSchema(DataType[] targetTypes, int[] tableFieldIndexes,
                              Object[] omittedFieldValues, int tableFieldCount, boolean partial) {
        this.targetTypes = targetTypes;
        this.tableFieldIndexes = tableFieldIndexes;
        this.omittedFieldValues = omittedFieldValues;
        this.tableFieldCount = tableFieldCount;
        this.partial = partial;
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
        if (columnNames == null || columnNames.length == 0) {
            throw new IllegalArgumentException(
                    "PaimonJniWriter requires explicit column names");
        }

        DataType[] targetTypes = new DataType[columnNames.length];
        int[] tableFieldIndexes = new int[columnNames.length];
        boolean[] specifiedFields = new boolean[tableType.getFieldCount()];
        for (int i = 0; i < columnNames.length; i++) {
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
        }

        Object[] omittedFieldValues = new Object[tableType.getFieldCount()];
        for (int i = 0; i < specifiedFields.length; i++) {
            if (!specifiedFields[i]) {
                DataField field = tableType.getFields().get(i);
                if (field.defaultValue() != null) {
                    omittedFieldValues[i] = DefaultValueUtils.convertDefaultValue(
                            field.type(), field.defaultValue());
                }
            }
        }
        return new PaimonWriteSchema(targetTypes, tableFieldIndexes, omittedFieldValues,
                tableType.getFieldCount(), columnNames.length != tableType.getFieldCount());
    }

    /** Paimon {@link DataType}s for each write column, in write order. */
    DataType[] targetTypes() {
        return targetTypes;
    }

    /** Expand one Arrow row to the full Paimon table-schema layout. */
    GenericRow tableRow(Object[][] columnValues, int rowIndex) {
        GenericRow row = new GenericRow(tableFieldCount);
        for (int i = 0; i < omittedFieldValues.length; i++) {
            if (omittedFieldValues[i] != null) {
                row.setField(i, omittedFieldValues[i]);
            }
        }
        for (int i = 0; i < tableFieldIndexes.length; i++) {
            row.setField(tableFieldIndexes[i], columnValues[i][rowIndex]);
        }
        return row;
    }

    boolean isPartial() {
        return partial;
    }
}

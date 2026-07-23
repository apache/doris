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

/**
 * Immutable mapping from Doris input columns to a Paimon table row.
 *
 * <p>The input may contain a subset of table columns in a different order. This class
 * resolves their types and table positions once, then converts each input row to the
 * table-schema layout expected by the Paimon writer.
 */
final class PaimonWriteSchema {
    private final DataType[] targetTypes;
    /** Maps Doris input-column position → Paimon table-schema position. */
    private final int[] tableFieldIndexes;
    private final int tableFieldCount;

    private PaimonWriteSchema(DataType[] targetTypes, int[] tableFieldIndexes,
                              int tableFieldCount) {
        this.targetTypes = targetTypes;
        this.tableFieldIndexes = tableFieldIndexes;
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

        return new PaimonWriteSchema(targetTypes, tableFieldIndexes, tableType.getFieldCount());
    }

    /** Paimon {@link DataType}s for each write column, in write order. */
    DataType[] targetTypes() {
        return targetTypes;
    }

    /** Expand one Arrow row to the full Paimon table-schema layout. */
    GenericRow tableRow(Object[][] columnValues, int rowIndex) {
        GenericRow row = new GenericRow(tableFieldCount);
        for (int i = 0; i < tableFieldIndexes.length; i++) {
            row.setField(tableFieldIndexes[i], columnValues[i][rowIndex]);
        }
        return row;
    }
}

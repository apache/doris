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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DefaultValueUtils;

import java.util.Arrays;

/**
 * Immutable mapping from Doris input columns to a Paimon table row.
 *
 * <p>The input may contain a subset of table columns in a different order. This class
 * resolves their types and table positions once, then converts each input row to the
 * table-schema layout expected by the Paimon writer.
 */
final class PaimonWriteSchema {

    /**
     * The synthetic row locator column an append-only row-level DML scan projects:
     * STRUCT&lt;file_path STRING, row_position BIGINT&gt;. Not a table column — it maps to no table
     * field and is consumed by the deletion-vector collector instead of the row itself. Must equal
     * the BE constant {@code BeConsts::PAIMON_ROWID_COL} and FE's declaration.
     */
    static final String ROWID_COL = "__DORIS_PAIMON_ROWID_COL__";

    /**
     * The merge-operation tag column of an operation-tagged (UPDATE/MERGE) row stream: a TINYINT whose
     * value is the fe-core {@code MergeOperation} number (1=INSERT, 2=DELETE, 3=UPDATE, 4=UPDATE_INSERT,
     * 5=UPDATE_DELETE). Synthetic like the row locator — it maps to no table field.
     */
    static final String OPERATION_COL = "operation";

    /** Sentinel in {@link #tableFieldIndexes} marking an input column with no table position. */
    private static final int NOT_A_TABLE_FIELD = -1;

    private final DataType[] targetTypes;
    /** Maps Doris input-column position → Paimon table-schema position. */
    private final int[] tableFieldIndexes;
    /** Paimon defaults for table fields omitted from the Doris input. */
    private final int[] omittedDefaultFieldIndexes;
    private final Object[] omittedDefaultValues;
    private final int tableFieldCount;
    /** Input position of the synthetic row-id column, or -1 when the scan does not project it. */
    private final int rowIdInputIndex;
    /** Input position of the merge-operation tag column, or -1 when the stream is not tagged. */
    private final int operationInputIndex;

    private PaimonWriteSchema(DataType[] targetTypes, int[] tableFieldIndexes,
            int[] omittedDefaultFieldIndexes, Object[] omittedDefaultValues, int tableFieldCount,
            int rowIdInputIndex, int operationInputIndex) {
        this.targetTypes = targetTypes;
        this.tableFieldIndexes = tableFieldIndexes;
        this.omittedDefaultFieldIndexes = omittedDefaultFieldIndexes;
        this.omittedDefaultValues = omittedDefaultValues;
        this.tableFieldCount = tableFieldCount;
        this.rowIdInputIndex = rowIdInputIndex;
        this.operationInputIndex = operationInputIndex;
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
        int rowIdInputIndex = -1;
        int operationInputIndex = -1;
        for (int i = 0; i < columnNames.length; i++) {
            if (OPERATION_COL.equalsIgnoreCase(columnNames[i])) {
                // The merge-operation tag: consumed by the writer to pick the RowKind, never written.
                if (operationInputIndex >= 0) {
                    throw new IllegalArgumentException("Duplicate Paimon operation column");
                }
                operationInputIndex = i;
                targetTypes[i] = DataTypes.TINYINT();
                tableFieldIndexes[i] = NOT_A_TABLE_FIELD;
                continue;
            }
            if (ROWID_COL.equalsIgnoreCase(columnNames[i])) {
                // The row locator is synthetic: it maps to no table field, and its Arrow STRUCT is
                // decoded with its own row type rather than a table column's.
                if (rowIdInputIndex >= 0) {
                    throw new IllegalArgumentException("Duplicate Paimon row-id column");
                }
                rowIdInputIndex = i;
                targetTypes[i] = RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"file_path", "row_position"});
                tableFieldIndexes[i] = NOT_A_TABLE_FIELD;
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
                rowIdInputIndex,
                operationInputIndex);
    }

    /** Paimon {@link DataType}s for each write column, in write order. */
    DataType[] targetTypes() {
        return targetTypes;
    }

    /** Whether the input carries the synthetic row-id column (an append-only DML scan does). */
    boolean hasRowId() {
        return rowIdInputIndex >= 0;
    }

    /** Whether the input carries the merge-operation tag column (an UPDATE/MERGE stream does). */
    boolean hasOperation() {
        return operationInputIndex >= 0;
    }

    /** The merge-operation number of one input row (the fe-core {@code MergeOperation} value). */
    byte operationValue(Object[] columnValues) {
        if (operationInputIndex < 0) {
            throw new IllegalStateException("The stream does not carry " + OPERATION_COL);
        }
        Object value = columnValues[operationInputIndex];
        if (value == null) {
            throw new IllegalStateException("A merge row carries a null operation tag");
        }
        return ((Number) value).byteValue();
    }

    /**
     * The row locator of one input row: a {@code GenericRow(file_path STRING, row_position BIGINT)},
     * or {@code null} when the scan projected the column but this row carries none.
     */
    GenericRow rowIdValue(Object[] columnValues) {
        if (rowIdInputIndex < 0) {
            throw new IllegalStateException("The scan did not project " + ROWID_COL);
        }
        return (GenericRow) columnValues[rowIdInputIndex];
    }

    /** Expand one input row to the full Paimon table-schema layout. */
    GenericRow tableRow(Object[] columnValues) {
        if (columnValues.length != tableFieldIndexes.length) {
            throw new IllegalArgumentException(
                    "Paimon input value count does not match write schema");
        }
        GenericRow row = new GenericRow(tableFieldCount);
        for (int i = 0; i < omittedDefaultFieldIndexes.length; i++) {
            row.setField(omittedDefaultFieldIndexes[i], omittedDefaultValues[i]);
        }
        for (int i = 0; i < tableFieldIndexes.length; i++) {
            if (tableFieldIndexes[i] == NOT_A_TABLE_FIELD) {
                // The synthetic row-id column addresses the row; it is not part of it.
                continue;
            }
            // Actual Doris input is applied last so an explicit NULL remains distinct
            // from an omitted field and retains Paimon's writer-side semantics.
            row.setField(tableFieldIndexes[i], columnValues[i]);
        }
        return row;
    }
}

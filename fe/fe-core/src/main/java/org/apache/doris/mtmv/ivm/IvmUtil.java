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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.collect.ImmutableList;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

/**
 * IVM (Incremental View Maintenance) utility class.
 * Centralizes IVM hidden column detection, naming, and ColumnDefinition factories.
 * Column name constants are defined in {@link Column}.
 */
public class IvmUtil {

    public static boolean isIvmHiddenColumn(String columnName) {
        return columnName != null && columnName.startsWith(Column.IVM_HIDDEN_COLUMN_PREFIX);
    }

    /**
     * Builds a null-safe deterministic row-id expression from key expressions:
     * <ul>
     *   <li>Empty list (scalar agg): returns {@code LargeIntLiteral(0)}</li>
     *   <li>Non-empty (grouped agg): returns
     *       {@code murmur_hash3_128(ifnull(k1,''), isnull(k1), ifnull(k2,''), isnull(k2), ...)}</li>
     * </ul>
     *
     * <p>Each key produces two hash arguments: {@code ifnull(cast(key AS VARCHAR), '')} to prevent
     * NULL propagation in the hash function, and {@code cast(isnull(key) AS VARCHAR)} to distinguish
     * groups that differ only in which positions are NULL (e.g. (NULL,'x') vs ('x',NULL)).
     *
     * <p>Used by both normalize (IvmNormalizeMtmv) and delta rewrite (IvmAggDeltaHandler)
     * to ensure row-id derivation is identical.
     */
    public static Expression buildRowIdHash(List<? extends Expression> keyExprs) {
        if (keyExprs.isEmpty()) {
            return new LargeIntLiteral(BigInteger.ZERO);
        }
        // For each key, emit two hash arguments:
        //   1. ifnull(cast(key AS VARCHAR), '') — coalesces NULL to '' so hash never receives NULL
        //   2. cast(isnull(key) AS VARCHAR)     — encodes NULL position to distinguish
        //      e.g. (NULL, '') from ('', NULL)
        ImmutableList.Builder<Expression> hashArgs = ImmutableList.builderWithExpectedSize(keyExprs.size() * 2);
        for (Expression key : keyExprs) {
            Expression asVarchar = (key.getDataType() instanceof CharacterType)
                    ? key : new Cast(key, VarcharType.SYSTEM_DEFAULT);
            hashArgs.add(new Nvl(asVarchar, new VarcharLiteral("")));
            hashArgs.add(new Cast(new IsNull(key), VarcharType.SYSTEM_DEFAULT));
        }
        return new MurmurHash3128(hashArgs.build());
    }

    /**
     * Generates a hidden column name for an IVM aggregate state.
     * Format: __DORIS_IVM_AGG_{ordinal}_{stateType}_COL__
     * Example: __DORIS_IVM_AGG_2_SUM_COL__, __DORIS_IVM_AGG_2_COUNT_COL__
     *
     * @param ordinal   the 0-based ordinal of the aggregate target in the MV query
     * @param stateType the state type (SUM, COUNT, etc.)
     */
    public static String ivmAggHiddenColumnName(int ordinal, String stateType) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "AGG_" + ordinal + "_" + stateType + "_COL__";
    }

    /**
     * Creates a hidden ColumnDefinition for the IVM row-id column. */
    public static ColumnDefinition newIvmRowIdColumnDefinition(DataType type, boolean isNullable) {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                Column.IVM_ROW_ID_COL, type, false, null, isNullable, Optional.empty(),
                "ivm row id hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);
        return columnDefinition;
    }

    /**
     * Creates a hidden ColumnDefinition for an IVM aggregate state.
     *
     * @param name       the hidden column name (e.g. __DORIS_IVM_AGG_0_SUM_COL__)
     * @param type       the data type of this state column
     * @param isNullable whether this state column can be null
     */
    public static ColumnDefinition newIvmAggHiddenColumnDefinition(String name, DataType type, boolean isNullable) {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                name, type, false, null, isNullable, Optional.empty(),
                "ivm aggregate hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);
        return columnDefinition;
    }

    /**
     * Finds the IVM row_id slot in the given output list.
     * Throws AnalysisException if not found or if multiple row_id slots are present.
     *
     * @param output the plan's output slots
     * @param context description of where this lookup happens (e.g. "left child of join")
     */
    public static Slot findRowIdSlot(List<Slot> output, String context) {
        Slot found = findRowIdSlotOrNull(output);
        if (found == null) {
            throw new AnalysisException("IVM: no row_id slot found in " + context);
        }
        return found;
    }

    /**
     * Finds the IVM row_id slot in the given output list, or returns null if not found.
     * Throws AnalysisException if multiple row_id slots are present.
     */
    public static Slot findRowIdSlotOrNull(List<Slot> output) {
        Slot found = null;
        for (Slot slot : output) {
            if (Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                if (found != null) {
                    throw new AnalysisException(
                            "IVM: multiple row_id slots found in plan output");
                }
                found = slot;
            }
        }
        return found;
    }
}

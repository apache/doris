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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;

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
     * Builds a deterministic row-id expression from key expressions:
     * <ul>
     *   <li>Empty list (scalar agg): returns {@code LargeIntLiteral(0)}</li>
     *   <li>Non-empty (grouped agg): returns {@code CAST(murmur_hash3_64(keys...) AS LARGEINT)}</li>
     * </ul>
     *
     * <p>Used by both normalize (IvmNormalizeMtmv) and delta rewrite (IvmAggDeltaStrategy)
     * to ensure row-id derivation is identical.
     */
    public static Expression buildRowIdHash(List<? extends Expression> keyExprs) {
        if (keyExprs.isEmpty()) {
            return new LargeIntLiteral(BigInteger.ZERO);
        }
        // murmur_hash3_64 only accepts VARCHAR/STRING arguments.  If every key is already a
        // CharacterType (VARCHAR or STRING) the cast is unnecessary; otherwise cast to VARCHAR
        // because this expression is constructed after type-coercion has already completed.
        boolean allCharacter = keyExprs.stream()
                .allMatch(e -> e.getDataType() instanceof CharacterType);
        Expression first = allCharacter ? keyExprs.get(0)
                : new Cast(keyExprs.get(0), VarcharType.SYSTEM_DEFAULT);
        Expression[] rest = keyExprs.subList(1, keyExprs.size()).stream()
                .map(e -> allCharacter ? e : (Expression) new Cast(e, VarcharType.SYSTEM_DEFAULT))
                .toArray(Expression[]::new);
        return new Cast(new MurmurHash364(first, rest), LargeIntType.INSTANCE);
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

    /** Creates a hidden ColumnDefinition for the IVM row-id column. */
    public static ColumnDefinition newIvmRowIdColumnDefinition(DataType type, boolean isNullable) {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                Column.IVM_ROW_ID_COL, type, false, null, isNullable, Optional.empty(),
                "ivm row id hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);
        return columnDefinition;
    }

    /**
     * Creates a hidden ColumnDefinition for the IVM group-level count.
     * Type is always BigInt (same as COUNT result), non-nullable.
     */
    public static ColumnDefinition newIvmCountColumnDefinition() {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                Column.IVM_AGG_COUNT_COL, BigIntType.INSTANCE, false, null, false, Optional.empty(),
                "ivm group count hidden column", false);
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
}

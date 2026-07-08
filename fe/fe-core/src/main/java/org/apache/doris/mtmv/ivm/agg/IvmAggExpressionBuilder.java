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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.catalog.Column;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DataType;

/**
 * Stateless expression builder shared by aggregate processors.
 *
 * <p>It centralizes the small expression idioms used by IVM aggregate rewrites so processors describe semantics instead
 * of repeating low-level {@code IF}, {@code COALESCE}, cast, and generated-name details.
 */
public class IvmAggExpressionBuilder {
    public static final IvmAggExpressionBuilder INSTANCE = new IvmAggExpressionBuilder();

    private IvmAggExpressionBuilder() {
    }

    /** Returns {@code expr} for inserts and {@code -expr} for deletes. */
    Expression signedDeltaValue(Expression expr, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new Subtract(zeroOf(expr.getDataType()), expr));
    }

    /** Returns {@code dml_factor} for non-NULL input values and zero for NULL input values. */
    Expression nonNullDeltaCountValue(Expression expr, Slot dmlFactorSlot) {
        return new If(new IsNull(expr), new TinyIntLiteral((byte) 0), dmlFactorSlot);
    }

    /** Guards a count-like expression from going negative while keeping an expression-typed result. */
    public Expression assertNonNegative(Expression expr, String message) {
        return new If(new AssertTrue(new GreaterThanEqual(expr,
                new BigIntLiteral(0)), new StringLiteral(message)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /** Builds {@code expr > 0}; used when a visible aggregate should be NULL or zero for empty groups. */
    Expression isPositive(Expression expr) {
        return new GreaterThan(expr, new BigIntLiteral(0));
    }

    /** Converts NULL arithmetic input to a typed zero. */
    Expression zeroIfNull(Slot slot) {
        return new Coalesce(slot, zeroOf(slot.getDataType()));
    }

    /** Finds one MV slot from the raw scan and converts NULL to zero for state merge arithmetic. */
    public Expression zeroIfNullMvSlot(LogicalOlapScan rawMvScan, String slotName) {
        return zeroIfNull(findSlotByName(rawMvScan, slotName));
    }

    /** Finds one slot by name from the raw MV scan output. */
    Slot findSlotByName(LogicalOlapScan rawMvScan, String slotName) {
        for (Slot slot : rawMvScan.getOutput()) {
            if (slotName.equals(slot.getName())) {
                return slot;
            }
        }
        throw new AnalysisException("IVM failed to find slot: " + slotName);
    }

    /** Builds a zero literal cast to the requested data type. */
    public Expression zeroOf(DataType dataType) {
        return new TinyIntLiteral((byte) 0).checkedCastTo(dataType);
    }

    /** Keeps an expression only for inserted rows; deleted rows become NULL and are ignored by MIN/MAX. */
    Expression insertOnlyValue(Expression expr, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /** Keeps an expression only for deleted rows; inserted rows become NULL and are ignored by MIN/MAX. */
    Expression deleteOnlyValue(Expression expr, Slot dmlFactorSlot) {
        return new If(new LessThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /** Builds the stable apply-time lookup key for one target and logical delta slot. */
    IvmAggDeltaSlotRef deltaSlotRef(IvmAggTarget target, IvmAggFunctionKind slotKind) {
        return new IvmAggDeltaSlotRef(target.getOrdinal(), slotKind);
    }

    /** Returns the generated column name used by the delta aggregate/top project for one logical delta slot. */
    String deltaColumnName(IvmAggTarget target, IvmAggFunctionKind slotKind) {
        switch (slotKind) {
            case COUNT:
                return target.stateColumnName(IvmAggStateKey.COUNT);
            case SUM:
                return target.stateColumnName(IvmAggStateKey.SUM);
            case MIN:
            case MAX:
            case BITMAP_UNION:
                return IvmUtil.ivmAggHiddenColumnName(target.getOrdinal(), slotKind.name());
            default:
                throw new IllegalArgumentException("Unsupported IVM aggregate delta slot: " + slotKind);
        }
    }

    /** Maps a persistent hidden state key to the delta slot kind that updates it. */
    IvmAggFunctionKind stateDeltaSlotKind(IvmAggStateKey stateKey) {
        return IvmAggFunctionKind.valueOf(stateKey.name());
    }

    /** Generated name for temporary delta outputs that should not become MV columns. */
    String transientDeltaColumnName(IvmAggTarget target, String slotName) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "TRANSIENT_" + target.getOrdinal()
                + "_" + slotName + "_COL__";
    }
}

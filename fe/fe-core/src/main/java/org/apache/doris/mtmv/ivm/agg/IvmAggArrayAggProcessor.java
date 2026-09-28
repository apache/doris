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

import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.ArrayAgg;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayFilter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayMap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CreateStruct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Processor for ARRAY_AGG(expr).
 *
 * <p>The visible array keeps NULL elements, so polarity columns cannot use the NULL-filtering
 * conditional-argument idiom (rows of the other polarity would leak into the array as NULL
 * elements). Instead every change row is packed into one struct field pair and aggregated once:
 *
 * <pre>array_agg(struct(dml_factor, elem))</pre>
 *
 * <p>The insert/delete polarity columns are then derived above the delta aggregate (see
 * {@link #appendDeltaTopProjectOutputs}) by splitting the packed pair array on the sign of the
 * factor field and projecting the elem field, one scalar pass per side. Both derived columns keep
 * the transient names the apply stage resolves, so the multiset merge is unchanged.
 */
class IvmAggArrayAggProcessor extends IvmAggArrayProcessor {
    private static final String PAIRS_SLOT = "ARRAY_PAIRS";
    /**
     * Field name of {@code struct(dml_factor, elem)} carrying the dml factor. {@link CreateStruct}
     * names unnamed fields {@code StructLiteral.COL_PREFIX + (i + 1)}, so the first field is "col1";
     * keep the string composed from the shared prefix so a rename cannot silently desync the
     * {@code element_at(x, ...)} field lookups below.
     */
    private static final String FACTOR_FIELD = StructLiteral.COL_PREFIX + "1";
    /**
     * Field name of {@code struct(dml_factor, elem)} carrying the packed element (second field).
     */
    private static final String ELEM_FIELD = StructLiteral.COL_PREFIX + "2";

    @Override
    public boolean supportsOriginalFunction(AggregateFunction function) {
        if (!(function instanceof ArrayAgg)) {
            return false;
        }
        // The packed delta aggregates struct(dml_factor, elem) per change row, and struct fields
        // cannot carry JSONB/VARIANT (CreateStruct rejects them). ARRAY_AGG over such element types
        // is not incrementally maintainable. Throw with the precise reason instead of returning
        // false: false would surface only the registry's generic "unsupported aggregate for IVM:
        // array_agg" error, which misleadingly blames the whole function while ARRAY_AGG itself is
        // supported. This check also runs while CREATE MATERIALIZED VIEW analyzes the query, so the
        // user sees the exact cause at create time.
        DataType elemType = function.child(0).getDataType();
        if (elemType.isJsonType() || elemType.isVariantType()) {
            throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED,
                    "IVM: ARRAY_AGG over JSONB/VARIANT element type " + elemType.toSql()
                            + " is not incrementally maintainable (the delta packs struct(dml_factor,"
                            + " elem) and struct fields cannot carry JSONB/VARIANT), create the MV"
                            + " with a COMPLETE refresh instead: " + function.toSql());
        }
        return true;
    }

    @Override
    public IvmAggFunctionKind handledFunctionKind() {
        return IvmAggFunctionKind.ARRAY_AGG;
    }

    @Override
    void appendDeltaAggregateOutputs(IvmAggTarget target, Slot dmlFactorSlot,
            List<NamedExpression> outputs, IvmAggExpressionBuilder ctx) {
        Expression elem = target.getExprArgs().get(0);
        outputs.add(new Alias(new ArrayAgg(new CreateStruct(dmlFactorSlot, elem)),
                ctx.transientDeltaColumnName(target, PAIRS_SLOT)));
    }

    @Override
    void appendDeltaTopProjectOutputs(IvmAggTarget target, Map<String, Slot> deltaAggOutputByName,
            List<NamedExpression> topOutputs, IvmAggExpressionBuilder ctx) {
        String pairsColumnName = ctx.transientDeltaColumnName(target, PAIRS_SLOT);
        Slot pairs = deltaAggOutputByName.get(pairsColumnName);
        if (pairs == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM agg delta rewrite failed to resolve packed array pairs output: "
                    + pairsColumnName + " for target " + target);
        }
        topOutputs.add(new Alias(splitPolarityPairs(true, pairs),
                ctx.transientDeltaColumnName(target, polaritySlotName(true))));
        topOutputs.add(new Alias(splitPolarityPairs(false, pairs),
                ctx.transientDeltaColumnName(target, polaritySlotName(false))));
    }

    /**
     * Splits the packed {@code array<struct<dml_factor, elem>>} into the elements of one polarity
     * side, mirroring a conditional aggregate without filtering NULL elements first:
     *
     * <pre>array_map(x -> element_at(x, 'col2'),
     *         array_filter(x -> element_at(x, 'col1') > 0, pairs))        // insert side
     *         array_filter(x -> element_at(x, 'col1') < 0, pairs))        // delete side</pre>
     *
     * <p>The factor field of a change row is never NULL, so every pair is classified exactly once
     * and rows with factor zero are dropped on both sides, as before.
     */
    private Expression splitPolarityPairs(boolean insertSide, Slot pairs) {
        ArrayItemReference pair = new ArrayItemReference("x", pairs);
        Expression factor = new ElementAt(pair.toSlot(), new StringLiteral(FACTOR_FIELD));
        Expression condition = insertSide ? new GreaterThan(factor, new TinyIntLiteral((byte) 0))
                : new LessThan(factor, new TinyIntLiteral((byte) 0));
        Expression filtered = new ArrayFilter(new Lambda(
                ImmutableList.of("x"), condition, ImmutableList.of(pair)));
        ArrayItemReference kept = new ArrayItemReference("y", filtered);
        Expression value = new ElementAt(kept.toSlot(), new StringLiteral(ELEM_FIELD));
        return new ArrayMap(new Lambda(ImmutableList.of("y"), value, ImmutableList.of(kept)));
    }
}

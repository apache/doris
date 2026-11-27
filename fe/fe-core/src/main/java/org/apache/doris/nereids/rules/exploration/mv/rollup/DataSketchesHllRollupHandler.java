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

package org.apache.doris.nereids.rules.exploration.mv.rollup;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.DataSketchesHllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.DataSketchesHllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Handles MV rollups targeting {@link DataSketchesHllUnion} for both
 * {@link DataSketchesHllUnionAgg} and cascading {@link DataSketchesHllUnion},
 * verifying first-argument equality and lg_max_k precision bounds (Kq_eff <= Kmv_eff).
 *
 * <p>Note: matching DataSketches HLL merge semantics in distributed two-phase aggregation,
 * rolling up dense pre-aggregated sketches switches from HIP to composite estimation and does not
 * guarantee bit-for-bit identical estimates or compact sketch bytes compared with single-stage
 * execution over raw sparse sketches; it preserves the {@code lg_max_k} precision cap and logical
 * sketch equivalence for subsequent union/estimation.
 *
 * <p>MTMV schema note: async MTMVs reject {@code VARBINARY} columns and narrow a column-0
 * {@code STRING} to {@code VARCHAR(65533)}, so MTMVs should use {@code STRING} sketch columns
 * ordered after {@code GROUP BY} key columns (required for dense {@code lg_max_k >= 16} sketches).
 */
public class DataSketchesHllRollupHandler extends AggFunctionRollUpHandler {

    public static final DataSketchesHllRollupHandler INSTANCE = new DataSketchesHllRollupHandler();
    private static final int DEFAULT_LG_MAX_K = 21;

    @Override
    public boolean canRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        if (!(queryAggregateFunction instanceof DataSketchesHllUnionAgg
                || queryAggregateFunction instanceof DataSketchesHllUnion)) {
            return false;
        }
        if (!(queryAggregateFunctionShuttled instanceof AggregateFunction)) {
            return false;
        }
        if (!super.canRollup(queryAggregateFunction, queryAggregateFunctionShuttled,
                mvExprToMvScanExprQueryBasedPair, mvExprToMvScanExprQueryBasedMap)) {
            return false;
        }
        Expression viewExpression = mvExprToMvScanExprQueryBasedPair.key();
        if (!(viewExpression instanceof DataSketchesHllUnion)) {
            return false;
        }
        AggregateFunction queryShuttled = (AggregateFunction) queryAggregateFunctionShuttled;
        DataSketchesHllUnion viewUnion = (DataSketchesHllUnion) viewExpression;
        if (!Objects.equals(queryShuttled.child(0), viewUnion.child(0))) {
            return false;
        }
        Integer queryLgMaxK = extractEffectiveLgMaxK(queryShuttled);
        Integer viewLgMaxK = extractEffectiveLgMaxK(viewUnion);
        if (queryLgMaxK == null || viewLgMaxK == null) {
            return false;
        }
        return queryLgMaxK <= viewLgMaxK;
    }

    @Override
    public Function doRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression rollupParam = mvExprToMvScanExprQueryBasedPair.value();
        if (rollupParam == null) {
            return null;
        }
        return ((RollUpTrait) queryAggregateFunction).constructRollUp(rollupParam);
    }

    @Nullable
    private static Integer extractEffectiveLgMaxK(AggregateFunction function) {
        if (function.arity() == 1) {
            return DEFAULT_LG_MAX_K;
        }
        Expression lgMaxK = function.child(1);
        if (!(lgMaxK instanceof IntegerLikeLiteral)) {
            // Conservatively refuse rewrite when lg_max_k is not a literal constant,
            // because precision compatibility Kq_eff <= Kmv_eff cannot be proven at plan time.
            return null;
        }
        return ((IntegerLikeLiteral) lgMaxK).getIntValue();
    }
}

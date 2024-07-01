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
import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Ndv;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllCardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Handle the aggregate functions roll up which are in AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP
 */
public class MappingRollupHandler extends AggFunctionRollUpHandler {

    public static MappingRollupHandler INSTANCE = new MappingRollupHandler();
    public static final Multimap<Function, Expression>
            AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP = ArrayListMultimap.create();

    static {
        // support roll up when count distinct is in query
        // the column type is not bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with bitmap_union, to_bitmap and cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Count(true, Any.INSTANCE),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));

        // support roll up when bitmap_union_count is in query
        // the column type is bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(Any.INSTANCE),
                new BitmapUnion(Any.INSTANCE));
        // the column type is not bitMap
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(Any.INSTANCE)),
                new BitmapUnion(new ToBitmap(Any.INSTANCE)));
        // with bitmap_union, to_bitmap and cast
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new BitmapUnionCount(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))),
                new BitmapUnion(new ToBitmap(new Cast(Any.INSTANCE, BigIntType.INSTANCE))));

        // support roll up when the column type is not hll
        // query is approx_count_distinct
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Ndv(Any.INSTANCE),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new Ndv(Any.INSTANCE),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_UNION_AGG
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnionAgg(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnionAgg(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_CARDINALITY
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(new HllHash(Any.INSTANCE))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(new HllHash(Any.INSTANCE))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllCardinality(new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT)))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllCardinality(new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT)))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // query is HLL_RAW_AGG or HLL_UNION
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(new HllHash(Any.INSTANCE)),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(Any.INSTANCE)));
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))),
                new HllUnion(new HllHash(new Cast(Any.INSTANCE, VarcharType.SYSTEM_DEFAULT))));

        // support roll up when the column type is hll
        // query is HLL_UNION_AGG
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnionAgg(Any.INSTANCE),
                new HllUnion(Any.INSTANCE));

        // query is HLL_CARDINALITY
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllCardinality(new HllUnion(Any.INSTANCE)),
                new HllUnion(Any.INSTANCE));

        // query is HLL_RAW_AGG or HLL_UNION
        AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.put(new HllUnion(Any.INSTANCE),
                new HllUnion(Any.INSTANCE));

    }

    @Override
    public boolean canRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        // handle complex functions roll up by mapping and combinator expression
        // eg: query is count(distinct param), mv sql is bitmap_union(to_bitmap(param))
        Expression viewExpression = mvExprToMvScanExprQueryBasedPair.key();
        if (!super.canRollup(queryAggregateFunction, queryAggregateFunctionShuttled,
                mvExprToMvScanExprQueryBasedPair, mvExprToMvScanExprQueryBasedMap)) {
            return false;
        }
        Function viewFunction = (Function) viewExpression;
        for (Map.Entry<Function, Collection<Expression>> equivalentFunctionEntry :
                AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.asMap().entrySet()) {
            if (equivalentFunctionEntry.getKey().equals(queryAggregateFunction)) {
                // check is have equivalent function or not
                for (Expression equivalentFunction : equivalentFunctionEntry.getValue()) {
                    if (!Any.equals(equivalentFunction, viewFunction)) {
                        continue;
                    }
                    // check param in query function is same as the view function
                    List<Expression> viewFunctionArguments = extractArguments(equivalentFunction, viewFunction);
                    List<Expression> queryFunctionArguments =
                            extractArguments(equivalentFunctionEntry.getKey(), queryAggregateFunction);
                    // check argument size,we only support roll up function which has only one argument currently
                    if (queryFunctionArguments.size() != 1 || viewFunctionArguments.size() != 1) {
                        continue;
                    }
                    if (Objects.equals(queryFunctionArguments.get(0), viewFunctionArguments.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Function doRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression rollupParam = mvExprToMvScanExprQueryBasedPair.value();
        return ((RollUpTrait) queryAggregateFunction).constructRollUp(rollupParam);
    }
}

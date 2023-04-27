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

package org.apache.doris.rewrite.mvrewrite;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.catalog.FunctionSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;

public class FunctionCallEqualRule implements MVExprEqualRule {

    public static MVExprEqualRule INSTANCE = new FunctionCallEqualRule();
    private static final ImmutableSetMultimap<String, String> columnAggTypeMatchFnName;

    static {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        builder.put("sum", "sum");
        builder.put("max", "max");
        builder.put("min", "min");
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION);
        builder.put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT);
        builder.put(FunctionSet.HLL_UNION, "hll_union_agg");
        builder.put(FunctionSet.HLL_UNION, "hll_union");
        builder.put(FunctionSet.HLL_UNION, "hll_raw_agg");
        builder.put(FunctionSet.TO_BITMAP, FunctionSet.TO_BITMAP);
        builder.put(FunctionSet.TO_BITMAP_WITH_CHECK, FunctionSet.TO_BITMAP_WITH_CHECK);
        builder.put(FunctionSet.HLL_HASH, FunctionSet.HLL_HASH);
        columnAggTypeMatchFnName = builder.build();
    }

    @Override
    public boolean equal(Expr queryExpr, Expr mvColumnExpr) {
        if ((!(queryExpr instanceof FunctionCallExpr)) || (!(mvColumnExpr instanceof FunctionCallExpr))) {
            return false;
        }
        FunctionCallExpr queryFn = (FunctionCallExpr) queryExpr;
        FunctionCallExpr mvColumnFn = (FunctionCallExpr) mvColumnExpr;
        // match fn name
        if (!columnAggTypeMatchFnName.get(mvColumnFn.getFnName().getFunction())
                .contains(queryFn.getFnName().getFunction().toLowerCase())) {
            return false;
        }
        // match children
        if (queryFn.getChildren().size() != mvColumnFn.getChildren().size()) {
            return false;
        }
        Preconditions.checkState(queryFn.getChildren().size() == 1);
        // remove cast to function
        Expr queryFnChild0 = queryFn.getChild(0);
        if (queryFnChild0 instanceof CastExpr) {
            queryFnChild0 = queryFnChild0.getChild(0);
        }
        Expr mvColumnFnChild0 = mvColumnFn.getChild(0);
        if (mvColumnFnChild0 instanceof CastExpr) {
            mvColumnFnChild0 = mvColumnFnChild0.getChild(0);
        }
        if (MVExprEquivalent.mvExprEqual(queryFnChild0, mvColumnFnChild0)) {
            return true;
        }
        return false;
    }
}

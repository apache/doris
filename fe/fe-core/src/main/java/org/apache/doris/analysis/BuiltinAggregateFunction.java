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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TAggregateFunction;
import org.apache.doris.thrift.TAggregationOp;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;

import java.util.ArrayList;

/**
 * Internal representation of a builtin aggregate function.
 */
public class BuiltinAggregateFunction extends Function {
    private final Operator op;
    // this is to judge the analytic function
    private boolean isAnalyticFn = false;

    public boolean isAnalyticFn() {
        return isAnalyticFn;
    }

    // TODO: this is not used yet until the planner understand this.
    private org.apache.doris.catalog.Type intermediateType;
    private boolean reqIntermediateTuple = false;

    public boolean isReqIntermediateTuple() {
        return reqIntermediateTuple;
    }

    public BuiltinAggregateFunction(Operator op, ArrayList<Type> argTypes,
            Type retType, org.apache.doris.catalog.Type intermediateType, boolean isAnalyticFn) {
        super(FunctionName.createBuiltinName(op.toString()), argTypes, retType, false);
        Preconditions.checkState(intermediateType != null);
        Preconditions.checkState(op != null);
        // may be no need to analyze
        // intermediateType.analyze();
        this.op = op;
        this.intermediateType = intermediateType;
        if (isAnalyticFn && !intermediateType.equals(retType)) {
            reqIntermediateTuple = true;
        }
        setBinaryType(TFunctionBinaryType.BUILTIN);
        this.isAnalyticFn = isAnalyticFn;
    }

    @Override
    public TFunction toThrift(Type realReturnType, Type[] realArgTypes, Boolean[] realArgTypeNullables) {
        TFunction fn = super.toThrift(realReturnType, realArgTypes, realArgTypeNullables);
        // TODO: for now, just put the op_ enum as the id.
        if (op == BuiltinAggregateFunction.Operator.FIRST_VALUE_REWRITE) {
            fn.setId(0);
        } else {
            fn.setId(op.thriftOp.ordinal());
        }
        fn.setAggregateFn(new TAggregateFunction(intermediateType.toThrift()));
        return fn;
    }

    public Operator op() {
        return op;
    }

    public org.apache.doris.catalog.Type getIntermediateType() {
        return intermediateType;
    }

    public void setIntermediateType(org.apache.doris.catalog.Type t) {
        intermediateType = t;
    }

    // TODO: this is effectively a catalog of builtin aggregate functions.
    // We should move this to something in the catalog instead of having it
    // here like this.
    public enum Operator {
        COUNT("COUNT", TAggregationOp.COUNT, Type.BIGINT),
        MIN("MIN", TAggregationOp.MIN, null),
        MAX("MAX", TAggregationOp.MAX, null),
        DISTINCT_PC("DISTINCT_PC", TAggregationOp.DISTINCT_PC, ScalarType.createVarcharType(64)),
        DISTINCT_PCSA("DISTINCT_PCSA", TAggregationOp.DISTINCT_PCSA, ScalarType.createVarcharType(64)),
        SUM("SUM", TAggregationOp.SUM, null),
        AVG("AVG", TAggregationOp.INVALID, null),
        GROUP_CONCAT("GROUP_CONCAT", TAggregationOp.GROUP_CONCAT, ScalarType.createVarcharType(16)),

        // NDV is the external facing name (i.e. queries should always be written with NDV)
        // The current implementation of NDV is hyperloglog (but we could change this without
        // external query changes if we find a better algorithm).
        NDV("NDV", TAggregationOp.HLL, ScalarType.createVarcharType(64)),
        HLL_UNION_AGG("HLL_UNION_AGG", TAggregationOp.HLL_C, ScalarType.createVarcharType(64)),
        BITMAP_UNION("BITMAP_UNION", TAggregationOp.BITMAP_UNION, ScalarType.createVarcharType(10)),
        COUNT_DISTINCT("COUNT_DISTINCT", TAggregationOp.COUNT_DISTINCT, Type.BIGINT),
        SUM_DISTINCT("SUM_DISTINCT", TAggregationOp.SUM_DISTINCT, null),
        LAG("LAG", TAggregationOp.LAG, null),
        FIRST_VALUE("FIRST_VALUE", TAggregationOp.FIRST_VALUE, null),
        LAST_VALUE("LAST_VALUE", TAggregationOp.LAST_VALUE, null),
        RANK("RANK", TAggregationOp.RANK, null),
        DENSE_RANK("DENSE_RANK", TAggregationOp.DENSE_RANK, null),
        ROW_NUMBER("ROW_NUMBER", TAggregationOp.ROW_NUMBER, null),
        LEAD("LEAD", TAggregationOp.LEAD, null),
        FIRST_VALUE_REWRITE("FIRST_VALUE_REWRITE", null, null),
        NTILE("NTILE", TAggregationOp.NTILE, null);

        private final String         description;
        private final TAggregationOp thriftOp;

        // The intermediate type for this function if it is constant regardless of
        // input type. Set to null if it can only be determined during analysis.
        private final org.apache.doris.catalog.Type intermediateType;

        Operator(String description, TAggregationOp thriftOp,
                org.apache.doris.catalog.Type intermediateType) {
            this.description = description;
            this.thriftOp = thriftOp;
            this.intermediateType = intermediateType;
        }

        @Override
        public String toString() {
            return description;
        }

        public TAggregationOp toThrift() {
            return thriftOp;
        }

        public org.apache.doris.catalog.Type intermediateType() {
            return intermediateType;
        }
    }
}

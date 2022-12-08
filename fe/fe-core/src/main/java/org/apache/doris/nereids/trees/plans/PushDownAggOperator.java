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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.thrift.TPushAggOp;

/**
 * use for push down agg without group by exprs to olap scan.
 */
public class PushDownAggOperator {
    public static PushDownAggOperator NONE = new PushDownAggOperator(TPushAggOp.NONE);
    public static PushDownAggOperator MIN_MAX = new PushDownAggOperator(TPushAggOp.MINMAX);
    public static PushDownAggOperator COUNT = new PushDownAggOperator(TPushAggOp.COUNT);
    public static PushDownAggOperator MIX = new PushDownAggOperator(TPushAggOp.MIX);

    private final TPushAggOp thriftOperator;

    private PushDownAggOperator(TPushAggOp thriftOperator) {
        this.thriftOperator = thriftOperator;
    }

    /**
     * merge operator.
     */
    public PushDownAggOperator merge(String functionName) {
        PushDownAggOperator newOne;
        if ("COUNT".equalsIgnoreCase(functionName)) {
            newOne = COUNT;
        } else {
            newOne = MIN_MAX;
        }
        if (this == NONE || this == newOne) {
            return newOne;
        } else {
            return MIX;
        }
    }

    public TPushAggOp toThrift() {
        return thriftOperator;
    }

    public boolean containsMinMax() {
        return this == MIN_MAX || this == MIX;
    }

    public boolean containsCount() {
        return this == COUNT || this == MIX;
    }

    @Override
    public String toString() {
        return thriftOperator.toString();
    }
}

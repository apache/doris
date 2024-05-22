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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.common.AnalysisException;

/**
 * MTMV Partition Expr Factory
 */
public class MTMVPartitionExprFactory {
    public static MTMVPartitionExprService getExprService(Expr expr) throws AnalysisException {
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("now async materialized view partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String fnName = functionCallExpr.getFnName().getFunction().toLowerCase();
        if ("date_trunc".equals(fnName)) {
            return new MTMVPartitionExprDateTrunc(functionCallExpr);
        }
        throw new AnalysisException("async materialized view partition not support function name: " + fnName);
    }
}

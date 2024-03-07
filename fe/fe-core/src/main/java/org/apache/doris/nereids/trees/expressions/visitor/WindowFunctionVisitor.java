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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.functions.window.CumeDist;
import org.apache.doris.nereids.trees.expressions.functions.window.DenseRank;
import org.apache.doris.nereids.trees.expressions.functions.window.FirstValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lag;
import org.apache.doris.nereids.trees.expressions.functions.window.LastValue;
import org.apache.doris.nereids.trees.expressions.functions.window.Lead;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.functions.window.PercentRank;
import org.apache.doris.nereids.trees.expressions.functions.window.Rank;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.functions.window.WindowFunction;

/** WindowFunctionVisitor. */
public interface WindowFunctionVisitor<R, C> {

    R visitWindowFunction(WindowFunction windowFunction, C context);

    default R visitDenseRank(DenseRank denseRank, C context) {
        return visitWindowFunction(denseRank, context);
    }

    default R visitFirstValue(FirstValue firstValue, C context) {
        return visitWindowFunction(firstValue, context);
    }

    default R visitLag(Lag lag, C context) {
        return visitWindowFunction(lag, context);
    }

    default R visitLastValue(LastValue lastValue, C context) {
        return visitWindowFunction(lastValue, context);
    }

    default R visitLead(Lead lead, C context) {
        return visitWindowFunction(lead, context);
    }

    default R visitNtile(Ntile ntile, C context) {
        return visitWindowFunction(ntile, context);
    }

    default R visitPercentRank(PercentRank percentRank, C context) {
        return visitWindowFunction(percentRank, context);
    }

    default R visitRank(Rank rank, C context) {
        return visitWindowFunction(rank, context);
    }

    default R visitRowNumber(RowNumber rowNumber, C context) {
        return visitWindowFunction(rowNumber, context);
    }

    default R visitCumeDist(CumeDist cumeDist, C context) {
        return visitWindowFunction(cumeDist, context);
    }
}

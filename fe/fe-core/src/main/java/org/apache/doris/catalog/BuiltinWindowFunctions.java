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

package org.apache.doris.catalog;

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

import com.google.common.collect.ImmutableList;

/**
 * Builtin aggregate functions.
 *
 * Note: Please ensure that this class only has some lists and no procedural code.
 *       It helps to be clear and concise.
 */
public class BuiltinWindowFunctions implements FunctionHelper {

    public final ImmutableList<WindowFunc> windowFunctions = ImmutableList.of(
            window(DenseRank.class, "dense_rank"),
            window(FirstValue.class, "first_value"),
            window(Lag.class, "lag"),
            window(LastValue.class, "last_value"),
            window(Lead.class, "lead"),
            window(Ntile.class, "ntile"),
            window(PercentRank.class, "percent_rank"),
            window(Rank.class, "rank"),
            window(RowNumber.class, "row_number"),
            window(CumeDist.class, "cume_dist")
    );

    public static final BuiltinWindowFunctions INSTANCE = new BuiltinWindowFunctions();

    // Note: Do not add any code here!
    private BuiltinWindowFunctions() {}
}

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

import org.apache.doris.nereids.trees.expressions.functions.Avg;
import org.apache.doris.nereids.trees.expressions.functions.Count;
import org.apache.doris.nereids.trees.expressions.functions.Max;
import org.apache.doris.nereids.trees.expressions.functions.Min;
import org.apache.doris.nereids.trees.expressions.functions.Substring;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.functions.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.Year;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Built-in functions.
 *
 * Note: Please ensure that this class only has some lists and no procedural code.
 *       It helps to be clear and concise.
 */
public class BuiltinFunctions implements FunctionHelper {
    public final List<ScalarFunc> scalarFunctions = ImmutableList.of(
            scalar(Substring.class, "substr", "substring"),
            scalar(WeekOfYear.class),
            scalar(Year.class)
    );

    public final ImmutableList<AggregateFunc> aggregateFunctions = ImmutableList.of(
            agg(Avg.class),
            agg(Count.class),
            agg(Max.class),
            agg(Min.class),
            agg(Sum.class)
    );
}

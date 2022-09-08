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

import org.apache.doris.nereids.trees.expressions.functions.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ScalarFunction;
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
 * New function registry for nereids.
 */
public class FunctionRegistry {
    public static final List<Named<ScalarFunction>> SCALAR_FUNCTIONS = ImmutableList.of(
            Substring.class,
            WeekOfYear.class,
            Year.class
    );

    public static final ImmutableList<Named<AggregateFunction>> AGGREGATE_FUNCTIONS = ImmutableList.of(
            Avg.class,
            Count.class,
            Max.class,
            Min.class,
            Sum.class
    );

    private static final

    private static class Named<T extends BoundFunction> {
        public final String name;
        public final Class<T> functionClass;

        public Named(String name, Class<T> functionClass) {
            this.name = name;
            this.functionClass = functionClass;
        }
    }
}

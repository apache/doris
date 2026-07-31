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

import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class AggregateFunctionVisitorTest {
    private static final String AGG_PACKAGE = "org.apache.doris.nereids.trees.expressions.functions.agg.";
    private static final String COMBINATOR_PACKAGE = "org.apache.doris.nereids.trees.expressions.functions.combinator.";

    @Test
    void testNullableAggregateFunctionsVisitNullableDefault() throws Exception {
        AggregateFunctionVisitor<String, Void> visitor = new AggregateFunctionVisitor<String, Void>() {
            @Override
            public String visitAggregateFunction(AggregateFunction function, Void context) {
                return "aggregate";
            }

            @Override
            public String visitNullableAggregateFunction(NullableAggregateFunction nullableAggregateFunction,
                    Void context) {
                return "nullable";
            }
        };

        for (Class<?> functionClass : nullableAggregateFunctionClasses()) {
            List<Method> visitorMethods = Arrays.stream(AggregateFunctionVisitor.class.getMethods())
                    .filter(method -> method.getParameterCount() == 2)
                    .filter(method -> method.getParameterTypes()[0].equals(functionClass))
                    .collect(Collectors.toList());
            Assertions.assertEquals(1, visitorMethods.size(), functionClass.getName());
            Assertions.assertEquals("nullable", visitorMethods.get(0).invoke(visitor, null, null),
                    functionClass.getName());
        }
    }

    private static List<Class<?>> nullableAggregateFunctionClasses() throws ClassNotFoundException {
        return Arrays.asList(
                aggregateClass("AIAgg"),
                aggregateClass("AnyValue"),
                aggregateClass("Avg"),
                aggregateClass("AvgWeighted"),
                aggregateClass("BoolAnd"),
                aggregateClass("BoolOr"),
                aggregateClass("BoolXor"),
                aggregateClass("Corr"),
                aggregateClass("CorrWelford"),
                aggregateClass("Covar"),
                aggregateClass("CovarSamp"),
                aggregateClass("ExponentialMovingAverage"),
                aggregateClass("GroupBitAnd"),
                aggregateClass("GroupBitOr"),
                aggregateClass("GroupBitXor"),
                aggregateClass("GroupBitmapXor"),
                aggregateClass("GroupConcat"),
                aggregateClass("Max"),
                aggregateClass("MaxBy"),
                aggregateClass("Median"),
                aggregateClass("Min"),
                aggregateClass("MinBy"),
                aggregateClass("MultiDistinctGroupConcat"),
                aggregateClass("MultiDistinctSum"),
                aggregateClass("Percentile"),
                aggregateClass("PercentileApprox"),
                aggregateClass("PercentileApproxWeighted"),
                aggregateClass("PercentileReservoir"),
                aggregateClass("Retention"),
                aggregateClass("Sem"),
                aggregateClass("SequenceMatch"),
                aggregateClass("Stddev"),
                aggregateClass("StddevSamp"),
                aggregateClass("Sum"),
                aggregateClass("TopN"),
                aggregateClass("TopNArray"),
                aggregateClass("TopNWeighted"),
                aggregateClass("Variance"),
                aggregateClass("VarianceSamp"),
                aggregateClass("WindowFunnel"),
                aggregateClass("WindowFunnelV2"),
                combinatorClass("ForEachCombinator"));
    }

    private static Class<?> aggregateClass(String name) throws ClassNotFoundException {
        return Class.forName(AGG_PACKAGE + name);
    }

    private static Class<?> combinatorClass(String name) throws ClassNotFoundException {
        return Class.forName(COMBINATOR_PACKAGE + name);
    }
}

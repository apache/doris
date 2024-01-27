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

package org.apache.doris.plugin.dialect.spark;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.ComplexFnCallTransformer;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

/**
 * DateTrunc complex function transformer
 */
public class DateTruncFnCallTransformer extends ComplexFnCallTransformer {

    // reference: https://spark.apache.org/docs/latest/api/sql/index.html#trunc
    // spark-sql support YEAR/YYYY/YY for year, support MONTH/MON/MM for month
    private static final ImmutableSet<String> YEAR = ImmutableSet.<String>builder()
            .add("YEAR")
            .add("YYYY")
            .add("YY")
            .build();

    private static final ImmutableSet<String> MONTH = ImmutableSet.<String>builder()
            .add("MONTH")
            .add("MON")
            .add("MM")
            .build();

    @Override
    public String getSourceFnName() {
        return "trunc";
    }

    @Override
    protected boolean check(String sourceFnName, List<Expression> sourceFnTransformedArguments,
                            ParserContext context) {
        return getSourceFnName().equalsIgnoreCase(sourceFnName) && (sourceFnTransformedArguments.size() == 2);
    }

    @Override
    protected Function transform(String sourceFnName, List<Expression> sourceFnTransformedArguments,
                                 ParserContext context) {
        VarcharLiteral fmtLiteral = (VarcharLiteral) sourceFnTransformedArguments.get(1);
        if (YEAR.contains(fmtLiteral.getValue().toUpperCase())) {
            return new UnboundFunction(
                    "date_trunc",
                    ImmutableList.of(sourceFnTransformedArguments.get(0), new VarcharLiteral("YEAR")));
        }
        if (MONTH.contains(fmtLiteral.getValue().toUpperCase())) {
            return new UnboundFunction(
                    "date_trunc",
                    ImmutableList.of(sourceFnTransformedArguments.get(0), new VarcharLiteral("MONTH")));
        }

        return new UnboundFunction(
                "date_trunc",
                ImmutableList.of(sourceFnTransformedArguments.get(0), sourceFnTransformedArguments.get(1)));
    }
}
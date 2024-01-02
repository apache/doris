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

package org.apache.doris.plugin.dialect.trino;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.ComplexFnCallTransformer;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * DateDiff complex function transformer
 */
public class DateDiffFnCallTransformer extends ComplexFnCallTransformer {

    private static final String SECOND = "second";
    private static final String HOUR = "hour";
    private static final String DAY = "day";
    private static final String MILLI_SECOND = "millisecond";

    @Override
    public String getSourceFnName() {
        return "date_diff";
    }

    @Override
    protected boolean check(String sourceFnName, List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        return getSourceFnName().equalsIgnoreCase(sourceFnName) && (sourceFnTransformedArguments.size() == 3);
    }

    @Override
    protected Function transform(String sourceFnName, List<Expression> sourceFnTransformedArguments,
            ParserContext context) {
        VarcharLiteral diffGranularity = (VarcharLiteral) sourceFnTransformedArguments.get(0);
        if (SECOND.equals(diffGranularity.getValue())) {
            return new UnboundFunction(
                    "seconds_diff",
                    ImmutableList.of(sourceFnTransformedArguments.get(1), sourceFnTransformedArguments.get(2)));
        }
        // TODO: support other date diff granularity
        return null;
    }
}

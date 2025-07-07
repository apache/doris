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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * In MySQL, LOG(col, e) is equivalent to LN(col), where e is Euler’s number (~2.71828).
 * Currently, MySQL also supports LOG(col) as a shorthand for LOG(col, e) (i.e., natural logarithm).
 * This conversion replaces LOG(col) with LN(col) when only a single argument is provided.
 */
public class LogToLn implements ExpressionPatternRuleFactory {

    public static final LogToLn INSTANCE = new LogToLn();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Log.class).then(LogToLn::rewrite)
                        .toRule(ExpressionRuleType.LOG_TO_LN)
        );
    }

    /** rewrite */
    public static Expression rewrite(Log log) {
        if (log.arity() == 1) {
            return new Ln(log.child(0));
        } else {
            return log;
        }
    }
}

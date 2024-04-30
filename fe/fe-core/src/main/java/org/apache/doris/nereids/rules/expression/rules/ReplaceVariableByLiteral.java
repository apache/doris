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

import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Variable;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * replace varaible to real expression
 */
public class ReplaceVariableByLiteral implements ExpressionPatternRuleFactory {
    public static ReplaceVariableByLiteral INSTANCE = new ReplaceVariableByLiteral();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
            matchesType(Variable.class).thenApply(ctx -> {
                StatementContext statementContext = ctx.cascadesContext.getStatementContext();
                Variable variable = ctx.expr;
                Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
                if (sqlCacheContext.isPresent()) {
                    sqlCacheContext.get().addUsedVariable(variable);
                }
                return variable.getRealExpression();
            })
        );
    }
}

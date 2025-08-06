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

package org.apache.doris.nereids.util;

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * FrontendConjunctsUtils
 */
public class FrontendConjunctsUtils {
    private static final Logger LOG = LogManager.getLogger(FrontendConjunctsUtils.class);

    public static List<Expression> convertToExpression(String conjuncts) {
        List<Expr> exprs = GsonUtils.GSON.fromJson(conjuncts, new TypeToken<List<Expr>>() {
        }.getType());
        return exprs.stream()
                .map(expr -> exprToExpression(expr))
                .collect(Collectors.toList());
    }

    public static Expression exprToExpression(Expr expr) {
        NereidsParser nereidsParser = new NereidsParser();
        return nereidsParser.parseExpression(expr.toSql());
    }

    public static boolean isFiltered(List<Expression> expressions, String columnName, Object value) {
        HashMap<String, Object> values = Maps.newHashMapWithExpectedSize(1);
        values.put(columnName.toLowerCase(), value);
        return isFiltered(expressions, values);
    }

    /**
     * isFiltered
     *
     * @param expressions expressions
     * @param values values
     * @return isFiltered
     */
    public static boolean isFiltered(List<Expression> expressions, Map<String, Object> values) {
        for (Expression expression : expressions) {
            if (isFiltered(expression, values)) {
                return true;
            }
        }
        return false;
    }

    /**
     * isFiltered
     *
     * @param expression expression
     * @param values values
     * @return isFiltered
     */
    public static boolean isFiltered(Expression expression, Map<String, Object> values) {
        try {
            AtomicBoolean containsAllColumn = new AtomicBoolean(true);
            Expression rewrittenExpr = expression.rewriteUp(expr -> {
                if (expr instanceof UnboundSlot) {
                    List<String> nameParts = ((UnboundSlot) expr).getNameParts();
                    if (!CollectionUtils.isEmpty(nameParts) && nameParts.size() == 1) {
                        String name = nameParts.get(0).toLowerCase();
                        if (values.containsKey(name)) {
                            return Literal.of(values.get(name));
                        } else {
                            containsAllColumn.set(false);
                        }
                    }
                }
                return expr;
            });
            // expression is: c1=v1 or c2=v2,
            // if values is {c1=v3}
            // we should not return true, because c2 may equals v2
            if (!containsAllColumn.get()) {
                return false;
            }
            Expression evaluate = FoldConstantRuleOnFE.evaluate(rewrittenExpr, null);
            if (evaluate instanceof BooleanLiteral && !((BooleanLiteral) evaluate).getValue()) {
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.warn("frontend conjuncts deal fail: expression: {}, values: {}", expression, values, e);
            return false;
        }

    }
}

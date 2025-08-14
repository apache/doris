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
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * FrontendConjunctsUtils
 */
public class FrontendConjunctsUtils {
    private static final Logger LOG = LogManager.getLogger(FrontendConjunctsUtils.class);
    private static List<String> nameParts;

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

    /**
     * Filter out the conjuncts that contain the current slotName.
     *
     * @param expressions expressions
     * @param slotName slotName
     * @return filterBySlotName
     */
    public static List<Expression> filterBySlotName(List<Expression> expressions, String slotName) {
        List<Expression> res = Lists.newArrayList();
        for (Expression expression : expressions) {
            if (containSlotName(expression, slotName)) {
                res.add(expression);
            }
        }
        return res;
    }

    private static boolean containSlotName(Expression expression, String slotName) {
        return expression.anyMatch(c -> {
            if (c instanceof UnboundSlot) {
                List<String> nameParts = ((UnboundSlot) c).getNameParts();
                if (!CollectionUtils.isEmpty(nameParts)) {
                    String name = nameParts.get(nameParts.size() - 1).toLowerCase();
                    return name.equalsIgnoreCase(slotName);
                }
            }
            return false;
        });
    }

    public static boolean isFiltered(List<Expression> expressions, String columnName, Object value) {
        TreeMap<String, Object> values = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        values.put(columnName, value);
        return isFiltered(expressions, values);
    }

    /**
     * isFiltered
     *
     * @param expressions expressions
     * @param values case insensitive map
     * @return isFiltered
     */
    public static boolean isFiltered(List<Expression> expressions, TreeMap<String, Object> values) {
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
     * @param values case insensitive map
     * @return isFiltered
     */
    public static boolean isFiltered(Expression expression, TreeMap<String, Object> values) {
        try {
            AtomicBoolean containsAllColumn = new AtomicBoolean(true);
            Expression rewrittenExpr = expression.rewriteUp(expr -> {
                if (expr instanceof UnboundSlot) {
                    List<String> nameParts = ((UnboundSlot) expr).getNameParts();
                    if (!CollectionUtils.isEmpty(nameParts)) {
                        String name = nameParts.get(nameParts.size() - 1).toLowerCase();
                        Object value = values.get(name);
                        if (value != null) {
                            return Literal.of(value);
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
            if (evaluate instanceof NullLiteral) {
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.warn("frontend conjuncts deal fail: expression: {}, values: {}", expression, values, e);
            return false;
        }

    }
}

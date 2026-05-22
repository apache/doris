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

import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Boolean Util tools.
 */
public class BooleanUtils {

    /**
     * rewrite in predicate element to be consistent with presto.
     */
    public static Expression processInPredicate(InPredicate inPredicate) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().getSqlDialect().equalsIgnoreCase(
                "presto")) {
            List<Expression> expressionList = inPredicate.children();
            Expression compareExpr = expressionList.get(0);
            if (compareExpr instanceof Cast && compareExpr.getDataType() instanceof CharacterType
                    && compareExpr.children().get(0).getDataType() instanceof BooleanType) {
                List<Expression> rewrittenExpressions = Lists.newArrayList();
                rewrittenExpressions.add(expressionList.get(0));
                for (int i = 1; i < expressionList.size(); i++) {
                    if (expressionList.get(i) instanceof CharLiteral) {
                        CharLiteral originLiteral = (CharLiteral) expressionList.get(i);
                        if (originLiteral.getValue().equalsIgnoreCase("true")) {
                            rewrittenExpressions.add(new CharLiteral("1", 1));
                        } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                            rewrittenExpressions.add(new CharLiteral("0", 1));
                        } else {
                            rewrittenExpressions.add(expressionList.get(i));
                        }
                    } else if (expressionList.get(i) instanceof VarcharLiteral) {
                        VarcharLiteral originLiteral = (VarcharLiteral) expressionList.get(i);
                        if (originLiteral.getValue().equalsIgnoreCase("true")) {
                            rewrittenExpressions.add(new VarcharLiteral("1"));
                        } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                            rewrittenExpressions.add(new VarcharLiteral("0"));
                        } else {
                            rewrittenExpressions.add(expressionList.get(i));
                        }
                    } else if (expressionList.get(i) instanceof StringLiteral) {
                        StringLiteral originLiteral = (StringLiteral) expressionList.get(i);
                        if (originLiteral.getValue().equalsIgnoreCase("true")) {
                            rewrittenExpressions.add(new StringLiteral("1"));
                        } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                            rewrittenExpressions.add(new StringLiteral("0"));
                        } else {
                            rewrittenExpressions.add(expressionList.get(i));
                        }
                    } else {
                        rewrittenExpressions.add(expressionList.get(i));
                    }
                }
                return inPredicate.withChildren(rewrittenExpressions);
            }
        }
        return inPredicate;
    }

    /**
     * rewrite equal predicate element to be consistent with presto.
     */
    public static Expression processEqualPredicate(Expression equalPredicate) {
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().getSqlDialect().equalsIgnoreCase(
                "presto")) {
            Expression compareExpr = equalPredicate.child(0);
            if (compareExpr instanceof Cast && compareExpr.getDataType() instanceof CharacterType
                    && compareExpr.children().get(0).getDataType() instanceof BooleanType) {
                List<Expression> rewrittenExpressions = Lists.newArrayList();
                rewrittenExpressions.add(compareExpr);
                if (equalPredicate.child(1) instanceof CharLiteral) {
                    CharLiteral originLiteral = (CharLiteral) equalPredicate.child(1);
                    if (originLiteral.getValue().equalsIgnoreCase("true")) {
                        rewrittenExpressions.add(new CharLiteral("1", 1));
                    } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                        rewrittenExpressions.add(new CharLiteral("0", 1));
                    } else {
                        rewrittenExpressions.add(equalPredicate.child(1));
                    }
                } else if (equalPredicate.child(1) instanceof VarcharLiteral) {
                    VarcharLiteral originLiteral = (VarcharLiteral) equalPredicate.child(1);
                    if (originLiteral.getValue().equalsIgnoreCase("true")) {
                        rewrittenExpressions.add(new VarcharLiteral("1"));
                    } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                        rewrittenExpressions.add(new VarcharLiteral("0"));
                    } else {
                        rewrittenExpressions.add(equalPredicate.child(1));
                    }
                } else if (equalPredicate.child(1) instanceof StringLiteral) {
                    StringLiteral originLiteral = (StringLiteral) equalPredicate.child(1);
                    if (originLiteral.getValue().equalsIgnoreCase("true")) {
                        rewrittenExpressions.add(new StringLiteral("1"));
                    } else if (originLiteral.getValue().equalsIgnoreCase("false")) {
                        rewrittenExpressions.add(new StringLiteral("0"));
                    } else {
                        rewrittenExpressions.add(equalPredicate.child(1));
                    }
                } else {
                    rewrittenExpressions.add(equalPredicate.child(1));
                }
                return equalPredicate.withChildren(rewrittenExpressions);
            }
        }
        return equalPredicate;
    }

}

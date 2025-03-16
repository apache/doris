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

package org.apache.doris.nereids.load;

import org.apache.doris.common.UserException;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * NereidsLoadUtils
 */
public class NereidsLoadUtils {
    /**
     * parse a expression list as 'select expr1, expr2,... exprn' into a nereids Expression List
     */
    public static List<Expression> parseExpressionSeq(String expressionSeq) throws UserException {
        LogicalPlan parsedPlan = new NereidsParser().parseSingle("SELECT " + expressionSeq);
        List<Expression> expressions = new ArrayList<>();
        parsedPlan.accept(new DefaultPlanVisitor<Void, List<Expression>>() {
            @Override
            public Void visitLogicalProject(LogicalProject<? extends Plan> logicalProject, List<Expression> exprs) {
                for (NamedExpression expr : logicalProject.getProjects()) {
                    if (expr instanceof UnboundAlias) {
                        exprs.add(expr.child(0));
                    } else if (expr instanceof UnboundSlot) {
                        exprs.add(expr);
                    } else {
                        // some error happens
                        exprs.clear();
                        break;
                    }
                }
                return super.visitLogicalProject(logicalProject, exprs);
            }
        }, expressions);
        if (expressions.isEmpty()) {
            throw new UserException("parse expression failed: " + expressionSeq);
        }
        return expressions;
    }
}

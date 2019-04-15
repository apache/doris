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

package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.functors.NOPClosure;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.operator.OptItemBinaryPredicate;
import org.apache.doris.optimizer.operator.OptItemCompoundPredicate;
import org.apache.doris.optimizer.operator.OptItemConst;
import org.apache.doris.optimizer.operator.OptItemSubqueryExists;
import org.apache.doris.optimizer.operator.OptItemSubqueryNotExists;
import org.apache.doris.optimizer.operator.OptItemSubqueryQuantified;
import org.apache.doris.optimizer.operator.OptLogicalInnerJoin;
import org.apache.doris.optimizer.operator.OptLogicalNAryJoin;
import org.apache.doris.optimizer.operator.OptLogicalProject;
import org.apache.doris.optimizer.operator.OptLogicalSelect;
import org.apache.doris.optimizer.operator.OptOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class OptUtils {
    public static final String HEADLINE_PREFIX = "->  ";
    public static final String DETAIL_PREFIX = "    ";
    private static int UT_ID_COUNTER = 1;

    public static int combineHash(int hash, int value) {
        return  ((hash << 5) ^ (hash >> 27)) ^ value;
    }

    public static int combineHash(int hash, Object obj) {
        return  ((hash << 5) ^ (hash >> 27)) ^ obj.hashCode();
    }

    public static int getUTOperatorId() {
        return UT_ID_COUNTER++;
    }

    public static boolean isSubquery(OptExpression expr) { return false; }
    public static boolean isBinaryPredicate(OptExpression expr) { return expr.getOp() instanceof OptItemBinaryPredicate; }
    public static boolean isExistentialSubquery(OptOperator op) {
        return op instanceof OptItemSubqueryExists || op instanceof OptItemSubqueryNotExists;
    }
    public static boolean isQuantifiedSubquery(OptOperator op) {
        return op instanceof OptItemSubqueryQuantified;
    }
    // check if a given operator is a logical unary operator
    public static boolean isLogicalUnary(OptOperator op) {
        return true;
    }
    public static OptExpression createConstBoolExpression(boolean value) {
        return OptExpression.create(OptItemConst.createBool(value));
    }
    public static OptExpression createConstBoolExpression(boolean value, boolean isNull) {
        return OptExpression.create(OptItemConst.createBool(value, isNull));
    }

    public static boolean isItemAnd(OptExpression expr) {
        return expr.getOp() instanceof OptItemCompoundPredicate &&
                ((OptItemCompoundPredicate) expr.getOp()).getOp() == CompoundPredicate.Operator.AND;
    }
    public static boolean isItemOr(OptExpression expr) {
        return expr.getOp() instanceof OptItemCompoundPredicate &&
                ((OptItemCompoundPredicate) expr.getOp()).getOp() == CompoundPredicate.Operator.OR;
    }
    public static boolean isItemNot(OptExpression expr) {
        return expr.getOp() instanceof OptItemCompoundPredicate &&
                ((OptItemCompoundPredicate) expr.getOp()).getOp() == CompoundPredicate.Operator.NOT;
    }
    public static boolean hasItemNotChild(OptExpression expr) {
        for (OptExpression input : expr.getInputs()) {
            if (isItemNot(input)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isItemConstBool(OptExpression expr, boolean value) {
        OptOperator op = expr.getOp();
        if (!(op instanceof OptItemConst)) {
            return false;
        }
        OptItemConst constOp = (OptItemConst) op;
        if (constOp.getReturnType().getPrimitiveType() != PrimitiveType.BOOLEAN) {
            return false;
        }
        return ((BoolLiteral) constOp.getLiteral()).getValue() == value;
    }
    public static boolean isItemConstTrue(OptExpression expr) {
        return isItemConstBool(expr, true);
    }
    public static boolean isItemConstFalse(OptExpression expr) {
        return isItemConstBool(expr, false);
    }
    public static OptExpression createCompoundPredicate(
            CompoundPredicate.Operator operator, List<OptExpression> inputs) {
        return OptExpression.create(new OptItemCompoundPredicate(operator), inputs);
    }

    public static OptExpression createBinaryPredicate(OptExpression left, OptExpression right,
                                                      BinaryPredicate.Operator operator) {
        return OptExpression.create(new OptItemBinaryPredicate(operator), left, right);
    }

    // Collapse the top two project nodes like this, if unable return NULL;
    //
    // clang-format off
    //  +--OptLogicalProject                                            <-- expr
    //      |--OptLogicalProject                                        <-- exprRel
    //      |  |--OptLogicalGet "t" ("t"), Columns: ["a" (0), "b" (1)]  <-- exprChildRel
    //      |  +--OptItemProjectList                                    <-- exprChildItem
    //      |     +--OptItemProjectElement "c" (9)
    //      |        +--OptItemFunc ()
    //      |           |--OptItemIdent "a" (0)
    //      |           +--OptItemConst ()
    //      +--OptItemProjectList                                     <-- exprItem
    //         +--OptItemProjectElement "d" (10)                      <-- exprPrE
    //            +--OptItemFunc ()
    //               |--OptItemIdent "b" (1)
    //               +--OptItemConst ()
    // clang-format on
    public static OptExpression collapseProjects(OptExpression expr) {
        if (!(expr.getOp() instanceof OptLogicalProject)) {
            return null;
        }
        OptExpression exprRel = expr.getInput(0);
        OptExpression exprItem = expr.getInput(1);
        if (!(exprRel.getOp() instanceof OptLogicalProject)) {
            return null;
        }

        OptExpression exprChildRel = exprRel.getInput(0);
        OptExpression exprChildItem = exprRel.getInput(1);
        // TODO(zc)
        return null;
    }

    public static OptExpression createLogicalSelect(OptExpression logical, OptExpression conj) {
        return OptExpression.create(new OptLogicalSelect(), logical, conj);
    }

    public static boolean isInnerJoin(OptExpression expr) {
        return expr.getOp() instanceof OptLogicalInnerJoin || expr.getOp() instanceof OptLogicalNAryJoin;
    }
    // append logical and scalar children of the given expression to the given arrays
    public static void collectChildren(OptExpression expr,
                                       List<OptExpression> logicalChildren,
                                       List<OptExpression> itemChildren) {
        for (OptExpression input : expr.getInputs()) {
            if (input.getOp().isLogical()) {
                logicalChildren.add(input);
            } else {
                itemChildren.add(input);
            }
        }
    }

    public static List<OptExpression> dedupExpressions(List<OptExpression> exprs) {
        return Lists.newArrayList(Sets.newHashSet(exprs));
    }
    public static OptExpression createNegate(OptExpression expr) {
        return OptExpression.create(new OptItemCompoundPredicate(CompoundPredicate.Operator.NOT), expr);
    }
}

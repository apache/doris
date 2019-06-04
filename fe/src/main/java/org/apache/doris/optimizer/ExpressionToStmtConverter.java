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
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CaseWhenClause;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.UserException;
import org.apache.doris.optimizer.operator.OptItemArithmetic;
import org.apache.doris.optimizer.operator.OptItemBinaryPredicate;
import org.apache.doris.optimizer.operator.OptItemCase;
import org.apache.doris.optimizer.operator.OptItemCast;
import org.apache.doris.optimizer.operator.OptItemColumnRef;
import org.apache.doris.optimizer.operator.OptItemCompoundPredicate;
import org.apache.doris.optimizer.operator.OptItemConst;
import org.apache.doris.optimizer.operator.OptItemFunctionCall;
import org.apache.doris.optimizer.operator.OptItemInPredicate;
import org.apache.doris.optimizer.operator.OptItemIsNullPredicate;
import org.apache.doris.optimizer.operator.OptItemLikePredicate;
import org.apache.doris.optimizer.operator.OptOperator;
import org.apache.doris.optimizer.operator.OptPhysicalMysqlScan;
import org.apache.doris.planner.MysqlScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// Convert expression to statement, used to convert optimized expression
// to a plan tree
public class ExpressionToStmtConverter {
    private static final Logger LOG = LogManager.getLogger(ExpressionToStmtConverter.class);

    private OptConverterCtx ctx;

    public ExpressionToStmtConverter(OptConverterCtx ctx) {
        this.ctx = ctx;
    }

    public PlanNode convertPhysical(OptExpression expr) {
        OptOperator op = expr.getOp();
        switch (op.getType()) {
            case OP_PHYSICAL_MYSQL_SCAN:
                return convertMysqlScan(expr);
        }
        return null;
    }

    public MysqlScanNode convertMysqlScan(OptExpression expr) {
//        OptPhysicalMysqlScan scanOp = (OptPhysicalMysqlScan) expr.getOp();
//        MysqlScanNode node = new MysqlScanNode();
        return null;
    }

    public Expr convertItem(OptExpression expr) throws UserException {
        OptOperator op = expr.getOp();
        switch (op.getType()) {
            case OP_ITEM_BINARY_PREDICATE:
                return convertBinaryPred(expr);
            case OP_ITEM_COMPOUND_PREDICATE:
                return convertCompoundPred(expr);
            case OP_ITEM_CONST:
                return convertConst(expr);
            case OP_ITEM_ARITHMETIC:
                return convertArithmetic(expr);
            case OP_ITEM_CASE:
                return convertCase(expr);
            case OP_ITEM_CAST:
                return convertCast(expr);
            case OP_ITEM_FUNCTION_CALL:
                return convertFunctionCall(expr);
            case OP_ITEM_AGG_FUNC:
                return convertAggregateFunction(expr);
            case OP_ITEM_COLUMN_REF:
                return convertColumnRef(expr);
            case OP_ITEM_IN_PREDICATE:
                return convertInPred(expr);
            case OP_ITEM_LIKE_PREDICATE:
                return convertLikePred(expr);
            case OP_ITEM_IS_NULL_PREDICATE:
                return convertIsNullPred(expr);
        }
        return null;
    }

    private Expr convertConst(OptExpression expr) {
        return ((OptItemConst) expr.getOp()).getLiteral();
    }

    private Expr convertIsNullPred(OptExpression expr) throws UserException {
        Expr child = convertItem(expr.getInput(0));
        OptItemIsNullPredicate pred = (OptItemIsNullPredicate) expr.getOp();
        return new IsNullPredicate(child, pred.isNotNull());
    }

    private Expr convertLikePred(OptExpression expr) throws UserException {
        Expr leftChild = convertItem(expr.getInput(0));
        Expr rightChild = convertItem(expr.getInput(0));
        OptItemLikePredicate pred = (OptItemLikePredicate) expr.getOp();
        return new LikePredicate(pred.getOp(), leftChild, rightChild);
    }

    private Expr convertInPred(OptExpression expr) throws UserException {
        Expr compareChild = convertItem(expr.getInput(0));

        List<Expr> children = Lists.newArrayList();
        for (int i = 1; i < expr.getInputs().size(); ++i) {
            children.add(convertItem(expr.getInput(i)));
        }

        OptItemInPredicate pred = (OptItemInPredicate) expr.getOp();
        return new InPredicate(compareChild, children, pred.isNotIn());
    }

    private Expr convertColumnRef(OptExpression expr) throws UserException {
        OptItemColumnRef columnRef = (OptItemColumnRef) expr.getOp();
        SlotDescriptor slotDesc = ctx.getSlotRef(columnRef.getRef().getId());
        if (slotDesc == null) {
            LOG.warn("fail to get SlotRef with ColumnRef, columnRef={}", columnRef.getRef());
            throw new UserException("Unknown column reference");
        }
        return new SlotRef(slotDesc);
    }

    private Expr convertAggregateFunction(OptExpression expr) {
        return null;
    }

    private Expr convertCast(OptExpression expr) throws UserException {
        Expr child = convertItem(expr.getInput(0));
        OptItemCast op = (OptItemCast) expr.getOp();
        return new CastExpr(op.getDestType(), child);
    }

    private Expr convertCase(OptExpression expr) throws UserException {
        OptItemCase caseOp = (OptItemCase) expr.getOp();
        Expr caseExpr = null;
        int idx = 0;
        if (caseOp.hasCase()) {
            caseExpr = convertItem(expr.getInput(idx));
            idx++;
        }
        int endIdx = expr.getInputs().size();
        Expr elseExpr = null;
        if (caseOp.hasElse()) {
            endIdx--;
            elseExpr = convertItem(expr.getInput(endIdx));
        }
        if ((endIdx - idx) % 2 != 0) {
            throw new UserException("");
        }
        List<CaseWhenClause> whenThenList = Lists.newArrayList();
        for (; idx < endIdx; idx += 2) {
            Expr whenExpr = convertItem(expr.getInput(idx));
            Expr thenExpr = convertItem(expr.getInput(idx + 1));
            whenThenList.add(new CaseWhenClause(whenExpr, thenExpr));
        }

        return new CaseExpr(caseExpr, whenThenList, elseExpr);
    }

    private Expr convertArithmetic(OptExpression expr) throws UserException {
        Expr leftChild = convertItem(expr.getInput(0));
        Expr rightChild = null;
        if (expr.getInputs().size() == 2) {
            rightChild = convertItem(expr.getInput(1));
        }
        OptItemArithmetic item = (OptItemArithmetic) expr.getOp();
        return new ArithmeticExpr(item.getOp(), leftChild, rightChild);
    }

    private Expr convertBinaryPred(OptExpression expr) throws UserException {
        Expr leftChild = convertItem(expr.getInput(0));
        Expr rightChild = convertItem(expr.getInput(1));

        OptItemBinaryPredicate pred = (OptItemBinaryPredicate) expr.getOp();
        return new BinaryPredicate(pred.getOp(), leftChild, rightChild);
    }

    private Expr convertCompoundPred(OptExpression expr) throws UserException {
        OptItemCompoundPredicate pred = (OptItemCompoundPredicate) expr.getOp();
        Expr leftChild = convertItem(expr.getInput(0));
        Expr rightChild = null;
        if (pred.getOp() != CompoundPredicate.Operator.NOT) {
            rightChild = convertItem(expr.getInput(1));
        }
        return new CompoundPredicate(pred.getOp(), leftChild, rightChild);
    }

    private Expr convertFunctionCall(OptExpression expr) throws UserException {
        OptItemFunctionCall funcOp = (OptItemFunctionCall) expr.getOp();

        List<Expr> children = Lists.newArrayList();
        for (OptExpression input : expr.getInputs()) {
            children.add(convertItem(input));
        }

        return new FunctionCallExpr(funcOp.getName(), children);
    }
}

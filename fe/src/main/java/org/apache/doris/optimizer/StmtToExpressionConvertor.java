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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.UnionStmt;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.optimizer.operator.OptItemArithmetic;
import org.apache.doris.optimizer.operator.OptItemCase;
import org.apache.doris.optimizer.operator.OptItemCast;
import org.apache.doris.optimizer.operator.OptItemColumnRef;
import org.apache.doris.optimizer.operator.OptItemCompoundPredicate;
import org.apache.doris.optimizer.operator.OptItemConst;
import org.apache.doris.optimizer.operator.OptItemFunctionCall;
import org.apache.doris.optimizer.operator.OptItemInPredicate;
import org.apache.doris.optimizer.operator.OptItemIsNullPredicate;
import org.apache.doris.optimizer.operator.OptItemLikePredicate;
import org.apache.doris.optimizer.operator.OptLogicalAggregate;
import org.apache.doris.optimizer.operator.OptLogicalScan;
import org.apache.doris.optimizer.operator.OptLogicalUnion;
import org.apache.doris.optimizer.operator.OptLogicalJoin;
import org.apache.doris.optimizer.operator.OptItemBinaryPredicate;

import java.util.List;
import java.util.Map;

// Used to convert QueryStmt to an OptExpression
public class StmtToExpressionConvertor {
    private OptColumnRefFactory columnRefFactory = new OptColumnRefFactory();
    private Map<Integer, OptColumnRef> slotIdToColumnRef = Maps.newHashMap();

    public OptExpression convertQuery(QueryStmt stmt) {
        OptExpression root;
        if (stmt instanceof SelectStmt) {
            root = convertSelect((SelectStmt) stmt);
        } else {
            Preconditions.checkArgument(stmt instanceof UnionStmt);
            root = convertUnion((UnionStmt) stmt);
        }
        return root;
    }

    public OptExpression convertUnion(UnionStmt stmt) {
        OptLogicalUnion unionOp = new OptLogicalUnion();
        List<OptExpression> inputs = Lists.newArrayList();
        for (UnionStmt.UnionOperand operand : stmt.getOperands()) {
            OptExpression input = convertQuery(operand.getQueryStmt());
            inputs.add(input);
        }
        return OptExpression.create(unionOp, inputs);
    }

    public OptExpression convertSelect(SelectStmt stmt) {
        // convert from
        OptExpression root = null;
        for (TableRef tableRef : stmt.getTableRefs()) {
            OptExpression rhsExpression = convertTableRef(tableRef);
            if (root != null) {
                root = convertJoin(root, rhsExpression);
            } else {
                root = rhsExpression;
            }
        }
        if (stmt.getAggInfo() != null) {
            root = convertAggregation(root, stmt.getAggInfo());
        }

        return root;
    }

    public OptExpression convertAggregation(OptExpression root, AggregateInfo aggInfo) {
        OptLogicalAggregate aggregate = new OptLogicalAggregate();
        return OptExpression.create(aggregate, root);
    }

    public OptExpression convertJoin(OptExpression outerExpression, OptExpression innerExpression) {
        OptLogicalJoin joinOp = new OptLogicalJoin();
        return OptExpression.create(joinOp, outerExpression, innerExpression);
    }

    public OptExpression convertTableRef(TableRef ref) {
        if (ref instanceof BaseTableRef) {
            return convertBaseTableRef((BaseTableRef) ref);
        } else {
            // inline view
        }
        return null;
    }

    public OptExpression convertBaseTableRef(BaseTableRef ref) {
        OptLogicalScan scan = new OptLogicalScan(ref);
        return OptExpression.create(scan);
    }

    public OptExpression convertExpr(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            return convertBinaryPredicate((BinaryPredicate) expr);
        } else if (expr instanceof ArithmeticExpr) {
            return convertArithmeticExpr((ArithmeticExpr) expr);
        } else if (expr instanceof SlotRef) {
            return convertSlotRef((SlotRef) expr);
        } else if (expr instanceof LiteralExpr) {
            return convertLiteral((LiteralExpr) expr);
        } else if (expr instanceof CompoundPredicate) {
            return convertCompoundPredicate((CompoundPredicate) expr);
        } else if (expr instanceof FunctionCallExpr) {
            return convertFunctionCall((FunctionCallExpr) expr);
        } else if (expr instanceof CastExpr) {
            return convertCastExpr((CastExpr) expr);
        } else if (expr instanceof CaseExpr) {
            return convertCaseExpr((CaseExpr) expr);
        } else if (expr instanceof IsNullPredicate) {
            return convertIsNullPredicate((IsNullPredicate) expr);
        } else if (expr instanceof LikePredicate) {
            return convertLikePredicate((LikePredicate) expr);
        } else if (expr instanceof InPredicate) {
            return convertInPredicate((InPredicate) expr);
        }
        return null;
    }

    private OptExpression convertBinaryPredicate(BinaryPredicate predicate) {
        OptExpression leftChild = convertExpr(predicate.getChild(0));
        OptExpression rightChild = convertExpr(predicate.getChild(0));

        OptItemBinaryPredicate op = new OptItemBinaryPredicate(predicate.getOp());
        return OptExpression.create(op, leftChild, rightChild);
    }

    private OptExpression convertArithmeticExpr(ArithmeticExpr expr) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (Expr child : expr.getChildren()) {
            inputs.add(convertExpr(child));
        }
        OptItemArithmetic op = new OptItemArithmetic(expr.getOp());
        return OptExpression.create(op, inputs);
    }

    private OptExpression convertSlotRef(SlotRef ref) {
        OptColumnRef columnRef = slotIdToColumnRef.get(ref.getSlotId().asInt());
        Preconditions.checkArgument(columnRef != null,
                "Can not find ColumnRef through ref, ref=" + ref.debugString());

        OptItemColumnRef op = new OptItemColumnRef(columnRef);
        return OptExpression.create(op);
    }

    private OptExpression convertLiteral(LiteralExpr literal) {
        OptItemConst op = new OptItemConst(literal);
        return OptExpression.create(op);
    }

    private OptExpression convertCompoundPredicate(CompoundPredicate predicate) {
        // NOT has one child and AND/OR may have may children
        List<OptExpression> inputs = Lists.newArrayList();
        for (Expr child : predicate.getChildren()) {
            inputs.add(convertExpr(child));
        }
        OptItemCompoundPredicate op = new OptItemCompoundPredicate(predicate.getOp());
        return OptExpression.create(op, inputs);
    }

    private OptExpression convertFunctionCall(FunctionCallExpr expr) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (Expr child : expr.getChildren()) {
            inputs.add(convertExpr(child));
        }
        OptItemFunctionCall op = new OptItemFunctionCall(expr);
        return OptExpression.create(op, inputs);
    }

    private OptExpression convertCastExpr(CastExpr expr) {
        OptExpression input = convertExpr(expr.getChild(0));
        OptItemCast op = new OptItemCast(expr.getType());
        return OptExpression.create(op, input);
    }

    private OptExpression convertCaseExpr(CaseExpr expr) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (Expr child : expr.getChildren()) {
            inputs.add(convertExpr(child));
        }
        OptItemCase op = new OptItemCase(expr.isHasCaseExpr(), expr.isHasElseExpr(), expr.getType());
        return OptExpression.create(op, inputs);
    }

    private OptExpression convertIsNullPredicate(IsNullPredicate expr) {
        OptExpression input = convertExpr(expr.getChild(0));
        OptItemIsNullPredicate op = new OptItemIsNullPredicate(expr.isNotNull());
        return OptExpression.create(op, input);
    }

    private OptExpression convertLikePredicate(LikePredicate expr) {
        OptExpression leftChild = convertExpr(expr.getChild(0));
        OptExpression rightChild = convertExpr(expr.getChild(0));

        OptItemLikePredicate op = new OptItemLikePredicate(expr.getOp());
        return OptExpression.create(op, leftChild, rightChild);
    }

    private OptExpression convertInPredicate(InPredicate expr) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (Expr child : expr.getChildren()) {
            inputs.add(convertExpr(child));
        }
        OptItemInPredicate op = new OptItemInPredicate(expr.isNotIn());
        return OptExpression.create(op, inputs);
    }
}

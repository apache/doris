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
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.optimizer.operator.*;

import java.util.List;
import java.util.Map;

// Used to convert QueryStmt to an OptExpression
public class StmtToExpressionConverter {
    private OptColumnRefFactory columnRefFactory;;
    private Map<Integer, OptColumnRef> slotIdToColumnRef = Maps.newHashMap();
    // map from OptColumnRef to SlotReference
    private Map<Integer, SlotRef> columnIdToSlotRef = Maps.newHashMap();

    private OptConverterCtx ctx;

    public StmtToExpressionConverter(OptConverterCtx ctx, OptColumnRefFactory columnRefFactory) {
        this.ctx = ctx;
        this.columnRefFactory = columnRefFactory;
    }

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
        List<OptExpression> inputs = Lists.newArrayList();
//        for (UnionStmt.UnionOperand operand : stmt.getOperands()) {
//            OptExpression input = convertQuery(operand.getQueryStmt());
//            inputs.add(input);
//        }
//        final OptColumnRefSet groupBy = new OptColumnRefSet();
//        for (Expr expr : stmt.getDistinctAggInfo().getGroupingExprs()) {
//            ItemUtils.collectSlotId(expr, groupBy, true);
//        }
//        final OptLogicalUnion op = new OptLogicalUnion(groupBy, !stmt.hasDistinctOps());
//        return OptExpression.create(op, inputs);
        return null;
    }

    public OptExpression convertSelect(SelectStmt stmt) {
        // convert from
        OptExpression root = null;
        for (TableRef tableRef : stmt.getTableRefs()) {
            OptExpression rhsExpression = convertTableRef(tableRef);
            if (root != null) {
                root = convertJoin(root, rhsExpression, tableRef);
            } else {
                root = rhsExpression;
            }
        }
        if (stmt.getAggInfo() != null) {
            root = convertAggregation(root, stmt.getAggInfo());
        }
        return root;
    }

    // Create OptLogicalAggregate and its children from AggregateInfo
    public OptExpression convertAggregation(OptExpression root, AggregateInfo aggInfo) {
        // 1. create projection from aggregateInfo
        List<OptExpression> projectElements = Lists.newArrayList();

        List<OptColumnRef> groupByColumns = Lists.newArrayList();
        for (int i = 0; i < aggInfo.getOutputTupleDesc().getSlots().size(); ++i) {
            SlotDescriptor slotDesc = aggInfo.getOutputTupleDesc().getSlots().get(i);
            OptColumnRef col = columnRefFactory.create(slotDesc.getType());
            Expr expr = null;
            if (i < aggInfo.getGroupingExprs().size()) {
                expr = aggInfo.getGroupingExprs().get(i);
                groupByColumns.add(col);
            } else {
                expr = aggInfo.getAggregateExprs().get(i - aggInfo.getGroupingExprs().size());
            }
            OptExpression projItem = convertExpr(expr);
            OptExpression projElement = OptExpression.create(new OptItemProjectElement(col), projItem);

            projectElements.add(projElement);
        }
        OptExpression projection = OptExpression.create(new OptItemProjectList(), projectElements);


        return OptExpression.create(
                new OptLogicalAggregate(groupByColumns, OptLogicalAggregate.AggType.GB_GLOBAL), root, projection);
    }
    
    public OptExpression convertJoin(OptExpression outerExpression, OptExpression innerExpression, TableRef tableRef) {
        OptLogicalJoin joinOp = null;
        if (tableRef.getJoinOp().isInnerJoin()) {
            joinOp = new OptLogicalInnerJoin();
        } else if (tableRef.getJoinOp().isLeftOuterJoin()) {
            joinOp = new OptLogicalLeftOuterJoin();
        } else if (tableRef.getJoinOp().isLeftSemiJoin()) {
            joinOp = new OptLogicalLeftSemiJoin();
        } else if (tableRef.getJoinOp().isLeftAntiJoin()) {
            joinOp = new OptLogicalLeftAntiJoin();
        } else {
            Preconditions.checkArgument(false, "Wrong join op:" + tableRef.getJoinOp());
        }
        return OptExpression.create(joinOp, outerExpression, innerExpression);
    }

    public OptExpression convertTableRef(TableRef ref) {
        if (ref instanceof BaseTableRef) {
            return convertBaseTableRef((BaseTableRef) ref);
        } else {
            return convertInlineView((InlineViewRef) ref);
        }
    }

    public OptExpression convertBaseTableRef(BaseTableRef ref) {
        List<OptColumnRef> columnRefs = ctx.convertTuple(ref.getDesc());
        OptLogical op = null;
        switch (ref.getTable().getType()) {
            case OLAP:
                op = new OptLogicalScan((OlapTable) ref.getTable(), columnRefs);
                break;
        }
        return OptExpression.create(op);
    }

    private OptExpression convertInlineView(InlineViewRef ref) {
        OptExpression childExpr = convertQuery(ref.getViewStmt());
        // New a projection node

        List<Expr> resultExprs = ref.getViewStmt().getResultExprs();

        List<OptColumnRef> columnRefs = ctx.convertTuple(ref.getDesc());

        List<OptExpression> projElements = Lists.newArrayList();
        for (int i = 0 ; i < columnRefs.size(); ++i) {
            OptExpression projItem = convertExpr(resultExprs.get(i));
            projElements.add(OptExpression.create(new OptItemProjectElement(columnRefs.get(i)), projItem));
        }
        OptExpression projList = OptExpression.create(new OptItemProjectList(), projElements);

        return OptExpression.create(new OptLogicalProject(), childExpr, projList);
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
        OptColumnRef columnRef = ctx.getColumnRef(ref.getSlotId().asInt());
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

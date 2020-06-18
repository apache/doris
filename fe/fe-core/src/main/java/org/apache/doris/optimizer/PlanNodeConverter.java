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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SingleNodePlanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class PlanNodeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(PlanNodeConverter.class);

    private final RelNode          root;
    private RelNode          from;
    private Filter           where;
    private Aggregate        groupBy;
    private Filter           having;
    private RelNode          select;
    private RelNode          orderLimit;

    private List<Column>           columns;
    private TupleDescriptor  tupleDescriptor;

    private Analyzer analyzer;
    private SingleNodePlanner planner;

    public PlanNodeConverter(RelNode root, Analyzer analyzer, SingleNodePlanner planner) {
        this.root = root;
        this.analyzer = analyzer;
        this.planner = planner;
    }

    class QBVisitor extends RelVisitor {
        public void handle(Filter filter) {
            RelNode child = filter.getInput();
            if (child instanceof Aggregate && !((Aggregate) child).getGroupSet().isEmpty()) {
                PlanNodeConverter.this.having = filter;
            } else {
                PlanNodeConverter.this.where = filter;
            }
        }

        public void handle(Project project) {
            if (PlanNodeConverter.this.select == null) {
                PlanNodeConverter.this.select = project;
            } else {
                PlanNodeConverter.this.from = project;
            }
        }

        public void handle(TableFunctionScan tableFunctionScan) {
            if (PlanNodeConverter.this.select == null) {
                PlanNodeConverter.this.select = tableFunctionScan;
            } else {
                PlanNodeConverter.this.from = tableFunctionScan;
            }
        }

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof TableScan) {
                PlanNodeConverter.this.from = node;
            } else if (node instanceof Filter) {
                handle((Filter) node);
            } else if (node instanceof Project) {
                handle((Project) node);
            } else if (node instanceof Join) {
                PlanNodeConverter.this.from = node;
            } else if (node instanceof Aggregate) {
                PlanNodeConverter.this.groupBy = (Aggregate) node;
            } else if (node instanceof Sort || node instanceof Exchange) {
                if (PlanNodeConverter.this.select != null) {
                    PlanNodeConverter.this.from = node;
                } else {
                    PlanNodeConverter.this.orderLimit = node;
                }
            }
            /*
             * once the source node is reached; stop traversal for this QB
             */
            if (PlanNodeConverter.this.from == null) {
                node.childrenAccept(this);
            }
        }
    }

    private Type calciteTypeToDorisType(RelDataType type) {
        String typeName = type.getSqlTypeName().getName();
        if (typeName.equals("INTEGER")) {
            return Type.INT;
        } else if (typeName.equals("CHAR")) {
            return Type.CHAR;
        } else {
            String msg = typeName + " can't supported";
            throw new UnsupportedOperationException(msg);
        }
    }

    private TupleDescriptor createTupleDescriptor(TableScan scan) throws AnalysisException {
        String dbName = analyzer.getDefaultDb();
        String tblName = scan.getTable().getQualifiedName().get(0).toLowerCase();
        TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(tblName);
        tupleDesc.setIsMaterialized(true);
        TableName tableName = new TableName(dbName, tblName);

        Database database = analyzer.getCatalog().getDb(dbName);
        if (database == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = database.getTable(tblName);
        if (table == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tblName);
        }
        TableRef tableRef = new BaseTableRef(new TableRef(tableName, null), table, tableName);
        tupleDesc.setRef(tableRef);
        tupleDesc.setTable(table);

        for (RelDataTypeField field : scan.deriveRowType().getFieldList()) {
            String colName = field.getKey();
            SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
            slotDesc.setLabel(colName);
            slotDesc.setType(calciteTypeToDorisType(field.getType()));
            slotDesc.setIsMaterialized(true);
            Column column = table.getColumn(colName);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, colName,
                   tableName);
            }
            slotDesc.setColumn(column);

            SlotRef slotRef = new SlotRef(tableName, colName);
            slotRef.setDesc(slotDesc);
            slotRef.setType(slotDesc.getType());
            //slotRef.analyze(analyzer);
        }
        tupleDesc.computeMemLayout();


        return tupleDesc;
    }

    private ScanNode convertSource(RelNode r)
            throws UserException {
        ScanNode scanNode = null;
        if (r instanceof TableScan) {
            TableScan scan = (TableScan)r;
            TupleDescriptor descriptor = createTupleDescriptor(scan);
            scanNode = new OlapScanNode(new PlanNodeId(0), descriptor,"OlapScanNode");

            Map<String, PartitionColumnFilter> columnFilters = Maps.newHashMap();
            List<Expr> conjuncts = analyzer.getUnassignedConjuncts(scanNode);

            for (Column column : descriptor.getTable().getBaseSchema()) {
                SlotDescriptor slotDesc = descriptor.getColumnSlot(column.getName());
                if (null == slotDesc) {
                    continue;
                }
                PartitionColumnFilter keyFilter = planner.createPartitionFilter(slotDesc, conjuncts);
                if (null != keyFilter) {
                    columnFilters.put(column.getName(), keyFilter);
                }
            }
            scanNode.setColumnFilters(columnFilters);
            scanNode.init(analyzer);
            this.tupleDescriptor = descriptor;
            this.columns = descriptor.getTable().getBaseSchema();
     } else {
            Preconditions.checkState(false);
        }
        return scanNode;
    }

    static class RexVisitor extends RexVisitorImpl<Expr> {
        private final RexBuilder rexBuilder;
        // this is to keep track of null literal which already has been visited
        private Map<RexLiteral, Boolean> nullLiteralMap ;
        private List<Column> columns;
        private TupleDescriptor tupleDescriptor;

        protected RexVisitor(List<Column> columns, TupleDescriptor descriptor, RexBuilder rexBuilder) {
            super(true);
            this.columns = columns;
            this.tupleDescriptor = descriptor;
            this.rexBuilder = rexBuilder;

            this.nullLiteralMap =
                    new TreeMap<>(new Comparator<RexLiteral>(){
                        // RexLiteral's equal only consider value and type which isn't sufficient
                        // so providing custom comparator which distinguishes b/w objects irrespective
                        // of value/type
                        @Override
                        public int compare(RexLiteral o1, RexLiteral o2) {
                            if(o1 == o2) {
                                return 0;
                            } else {
                                return 1;
                            }
                        }
                    });
        }

        @Override
        public Expr visitFieldAccess(RexFieldAccess fieldAccess) {
            // not implement
            Preconditions.checkState(false);
            return null;
        }

        @Override
        public Expr visitInputRef(RexInputRef inputRef) {
            Column column = columns.get(inputRef.getIndex());
            SlotDescriptor slotDescriptor = null;
            for (SlotDescriptor iter: tupleDescriptor.getSlots()) {
                if (iter.getColumn().equals(column)) {
                    slotDescriptor = iter;
                    break;
                }
            }
            Preconditions.checkState(slotDescriptor != null);
            return new SlotRef(slotDescriptor);
        }

        @Override
        public Expr visitLiteral(RexLiteral literal) {
            SqlTypeName typeName = literal.getType().getSqlTypeName();
            switch (typeName) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    try {
                        return new IntLiteral(((BigDecimal)literal.getValue()).longValue(), Type.INT);
                    } catch (AnalysisException e) {
                        LOG.warn("", e);
                        throw new NullPointerException();
                    }
                default:
                    throw new NullPointerException();
            }
        }

        private BinaryPredicate.Operator sqlKindrToBinaryOperator(SqlKind sqlKind) {
            switch (sqlKind) {
                case EQUALS:
                    return BinaryPredicate.Operator.EQ;
                case NOT_EQUALS:
                    return BinaryPredicate.Operator.NE;
                case LESS_THAN:
                    return BinaryPredicate.Operator.LT;
                case GREATER_THAN:
                    return BinaryPredicate.Operator.GT;
                case LESS_THAN_OR_EQUAL:
                    return BinaryPredicate.Operator.LE;
                case GREATER_THAN_OR_EQUAL:
                    return BinaryPredicate.Operator.GE;
                default:
                    // the stmt is not executed.
                    throw new NullPointerException();
            }

        }

        @Override
        public Expr visitCall(RexCall call) {
            if (!deep) {
                return null;
            }

            SqlOperator op = call.getOperator();
            List<Expr> exprList = Lists.newArrayList();
            switch (op.kind) {
                case EQUALS:
                case NOT_EQUALS:
                case LESS_THAN:
                case GREATER_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    if (rexBuilder != null && RexUtil.isReferenceOrAccess(call.operands.get(1), true) &&
                            RexUtil.isLiteral(call.operands.get(0), true)) {
                        // Swap to get reference on the left side
                        return visitCall((RexCall) RexUtil.invert(rexBuilder, call));
                    } else {
                        for (RexNode operand : call.operands) {
                            exprList.add(operand.accept(this));
                        }
                        Preconditions.checkState(call.operands.size() == 2);
                        return new BinaryPredicate(sqlKindrToBinaryOperator(op.kind), exprList.get(0), exprList.get(1));
                    }
                case OR:
                    Expr expr;
                    for (RexNode operand : call.operands) {
                        exprList.add(operand.accept(this));
                    }
                    expr = new CompoundPredicate(CompoundPredicate.Operator.OR,
                            exprList.get(0),
                            exprList.get(1));
                    for (int i = 2; i < exprList.size(); i++) {
                        expr = new CompoundPredicate(CompoundPredicate.Operator.OR, expr, exprList.get(i));
                    }
                    return expr;
                default:
                    for (RexNode operand : call.operands) {
                        exprList.add(operand.accept(this));
                    }
            }
            return null;
        }
    }

    public PlanNode convert(List<Expr> resultExprs, List<String> colLabels)  {
        /*
         * 1. Walk RelNode Graph; note from, where, gBy.. nodes.
         */
        new QBVisitor().go(root);

        /*
         * 2. convert from node.
         */
        PlanNode node = null;
        try {
            node = convertSource(from);
        } catch (UserException e) {
            e.printStackTrace();
            return null;
        }

        /*
         * 3. convert filterNode
         */
        if (where != null) {
            List<Expr> conjuncts = Lists.newArrayList();
            boolean bAndPredicate = false;
            RexNode rexNode = where.getCondition();
            if (rexNode instanceof RexCall) {
                RexCall whereConjuncts = (RexCall) rexNode;
                if (whereConjuncts.getOperator().getKind() == SqlKind.AND) {
                    for (RexNode whereConjunct : whereConjuncts.getOperands()) {
                        Expr expr = whereConjunct.accept(new RexVisitor(columns, tupleDescriptor, root.getCluster().getRexBuilder()));
                        conjuncts.add(expr);
                    }
                    bAndPredicate = true;
                }
            }
            if (!bAndPredicate) {
                Expr expr = where.getCondition().accept(new RexVisitor(columns, tupleDescriptor, root.getCluster().getRexBuilder()));
                conjuncts.add(expr);
            }
            node.addConjuncts(conjuncts);
        }

        /*
         * 4. Project
         */
        if (select instanceof Project) {
            List<RexNode> childExps = ((Project) select).getChildExps();
            if (childExps.isEmpty()) {
                LOG.warn("childExpr of Project is null");
                return null;
            } else {
                for (RexNode r : childExps) {
                    Expr expr = r.accept(new RexVisitor(columns, tupleDescriptor, root.getCluster().getRexBuilder()));
                    if (expr == null) {
                        LOG.warn("transport expr fail, RexNode={}", r);
                        return null;
                    }
                    resultExprs.add(expr);
                    colLabels.add(expr.toSql());
                }
            }
        }
        return node;
    }
}


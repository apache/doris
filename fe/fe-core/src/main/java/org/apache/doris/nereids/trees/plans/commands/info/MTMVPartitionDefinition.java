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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Expr.java
// and modified by Doris

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mtmv.MTMVPartitionExprFactory;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils.RelatedTableInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MTMVPartitionDefinition
 */
public class MTMVPartitionDefinition {
    private MTMVPartitionType partitionType;
    private String partitionCol;
    private Expression functionCallExpression;

    /**
     * analyzeAndTransferToMTMVPartitionInfo
     *
     * @param planner planner
     * @param ctx ctx
     * @param logicalQuery logicalQuery
     * @return MTMVPartitionInfo
     */
    public MTMVPartitionInfo analyzeAndTransferToMTMVPartitionInfo(NereidsPlanner planner, ConnectContext ctx,
            LogicalPlan logicalQuery) {
        MTMVPartitionInfo mtmvPartitionInfo = new MTMVPartitionInfo(partitionType);
        if (this.partitionType == MTMVPartitionType.SELF_MANAGE) {
            return mtmvPartitionInfo;
        }
        String partitionColName;
        if (this.partitionType == MTMVPartitionType.EXPR) {
            Expr expr;
            if (functionCallExpression instanceof UnboundFunction) {
                UnboundFunction function = (UnboundFunction) functionCallExpression;
                expr = new FunctionCallExpr(function.getName(),
                        new FunctionParams(convertToLegacyArguments(function.children())));
            } else {
                throw new AnalysisException(
                        "unsupported auto partition expr " + functionCallExpression.toString());
            }
            partitionColName = getColNameFromExpr(expr);
            mtmvPartitionInfo.setExpr(expr);
        } else {
            partitionColName = this.partitionCol;
        }
        mtmvPartitionInfo.setPartitionCol(partitionColName);
        RelatedTableInfo relatedTableInfo = getRelatedTableInfo(planner, ctx, logicalQuery, partitionColName);
        mtmvPartitionInfo.setRelatedCol(relatedTableInfo.getColumn());
        mtmvPartitionInfo.setRelatedTable(relatedTableInfo.getTableInfo());
        if (this.partitionType == MTMVPartitionType.EXPR) {
            try {
                MTMVPartitionExprFactory.getExprService(mtmvPartitionInfo.getExpr()).analyze(mtmvPartitionInfo);
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
        return mtmvPartitionInfo;
    }

    /**
     * getColNameFromExpr
     *
     * @param expr expr
     * @return String
     */
    public static String getColNameFromExpr(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException(
                    "auto create partition only support function call expr is: "
                            + MTMVPartitionInfo.MTMV_PARTITION_FUNCTIONS);
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        List<Expr> paramsExpr = functionCallExpr.getParams().exprs();
        String name = functionCallExpr.getFnName().getFunction();
        if (MTMVPartitionInfo.MTMV_PARTITION_FUNCTIONS.contains(name)) {
            for (Expr param : paramsExpr) {
                if (param instanceof SlotRef) {
                    return ((SlotRef) param).getColumnName();
                }
            }
            throw new AnalysisException("can not find colName");
        } else {
            throw new AnalysisException(
                    "auto create partition only support function call expr is: "
                            + MTMVPartitionInfo.MTMV_PARTITION_FUNCTIONS);
        }
    }

    private RelatedTableInfo getRelatedTableInfo(NereidsPlanner planner, ConnectContext ctx, LogicalPlan
            logicalQuery,
            String partitionColName) {
        CascadesContext cascadesContext = planner.getCascadesContext();
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        Set<String> tempDisableRules = sessionVariable.getDisableNereidsRuleNames();
        // Should not make table without data to empty relation when analyze the related table,
        // so add disable rules
        sessionVariable.setDisableNereidsRules(CreateMTMVInfo.MTMV_PLANER_DISABLE_RULES);
        cascadesContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        try {
            Plan mvRewrittenPlan =
                    planner.plan(logicalQuery, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
            Optional<RelatedTableInfo> relatedTableInfo = MaterializedViewUtils
                    .getRelatedTableInfo(partitionColName, mvRewrittenPlan);
            if (!relatedTableInfo.isPresent() || !relatedTableInfo.get().isPctPossible()) {
                throw new AnalysisException("Unable to find a suitable base table for partitioning");
            }
            MTMVRelatedTableIf mtmvBaseRealtedTable = MTMVUtil.getRelatedTable(relatedTableInfo.get().getTableInfo());
            Set<String> partitionColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            try {
                partitionColumnNames.addAll(mtmvBaseRealtedTable.getPartitionColumnNames());
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            if (!partitionColumnNames.contains(relatedTableInfo.get().getColumn())) {
                throw new AnalysisException("error related column: " + relatedTableInfo.get().getColumn());
            }
            if (!(mtmvBaseRealtedTable instanceof HMSExternalTable)
                    && partitionColumnNames.size() != 1) {
                throw new AnalysisException("only hms table support multi column partition.");
            }
            return relatedTableInfo.get();
        } finally {
            // after operate, roll back the disable rules
            sessionVariable.setDisableNereidsRules(String.join(",", tempDisableRules));
            cascadesContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        }
    }

    private static List<Expr> convertToLegacyArguments(List<Expression> children) {
        return children.stream().map(child -> {
            if (child instanceof UnboundSlot) {
                return new SlotRef(null, ((UnboundSlot) child).getName());
            } else if (child instanceof Literal) {
                return new StringLiteral(((Literal) child).getStringValue());
            } else {
                throw new AnalysisException("unsupported argument " + child.toString());
            }
        }).collect(Collectors.toList());
    }

    public MTMVPartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(MTMVPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartitionCol() {
        return partitionCol;
    }

    public void setPartitionCol(String partitionCol) {
        this.partitionCol = partitionCol;
    }

    public Expression getFunctionCallExpression() {
        return functionCallExpression;
    }

    public void setFunctionCallExpression(Expression functionCallExpression) {
        this.functionCallExpression = functionCallExpression;
    }
}

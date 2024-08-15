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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MTMVPartitionDefinition
 */
public class MTMVPartitionDefinition {
    public static final String PARTITION_BY_FUNCTION_NAME = "date_trunc";
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
        String timeUnit;
        if (this.partitionType == MTMVPartitionType.EXPR) {
            String functionName = ((UnboundFunction) functionCallExpression).getName();
            if (functionCallExpression instanceof UnboundFunction
                    && functionName.equalsIgnoreCase(PARTITION_BY_FUNCTION_NAME)) {
                partitionColName = functionCallExpression.getArgument(0) instanceof UnboundSlot
                        ? ((UnboundSlot) functionCallExpression.getArgument(0)).getName() : null;
                timeUnit = functionCallExpression.getArguments().get(1).isLiteral()
                        ? ((Literal) functionCallExpression.getArgument(1)).getStringValue() : null;
            } else {
                throw new AnalysisException(
                        "unsupported auto partition expr " + functionCallExpression.toString());
            }
        } else {
            partitionColName = this.partitionCol;
            timeUnit = null;
        }
        mtmvPartitionInfo.setPartitionCol(partitionColName);
        RelatedTableInfo relatedTableInfo = getRelatedTableInfo(planner, ctx, logicalQuery, partitionColName, timeUnit);
        mtmvPartitionInfo.setRelatedCol(relatedTableInfo.getColumn());
        mtmvPartitionInfo.setRelatedTable(relatedTableInfo.getTableInfo());
        if (relatedTableInfo.getPartitionExpression().isPresent()) {
            // Set mv partition expr by relatedTableInfo, this is used for partition rollup and so on
            if (relatedTableInfo.getPartitionExpression().get().getExpressionName()
                    .equalsIgnoreCase(PARTITION_BY_FUNCTION_NAME)) {
                DateTrunc dateTrunc = (DateTrunc) relatedTableInfo.getPartitionExpression().get();
                // todo use new expression?
                mtmvPartitionInfo.setExpr(new FunctionCallExpr(dateTrunc.getName(),
                        new FunctionParams(convertToLegacyArguments(dateTrunc.children()))));
                mtmvPartitionInfo.setPartitionType(MTMVPartitionType.EXPR);
                this.partitionType = MTMVPartitionType.EXPR;
            }
        }
        if (this.partitionType == MTMVPartitionType.EXPR) {
            try {
                MTMVPartitionExprFactory.getExprService(mtmvPartitionInfo.getExpr()).analyze(mtmvPartitionInfo);
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
        return mtmvPartitionInfo;
    }

    private RelatedTableInfo getRelatedTableInfo(NereidsPlanner planner, ConnectContext ctx, LogicalPlan
            logicalQuery,
            String partitionColName,
            String timeUnit) {
        CascadesContext cascadesContext = planner.getCascadesContext();
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        Set<String> tempDisableRules = sessionVariable.getDisableNereidsRuleNames();
        // Should not make table without data to empty relation when analyze the related table,
        // so add disable rules
        sessionVariable.setDisableNereidsRules(CreateMTMVInfo.MTMV_PLANER_DISABLE_RULES);
        cascadesContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        try {
            Plan mvRewrittenPlan =
                    planner.planWithLock(logicalQuery, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
            RelatedTableInfo relatedTableInfo = MaterializedViewUtils
                    .getRelatedTableInfo(partitionColName, timeUnit, mvRewrittenPlan, cascadesContext);
            if (!relatedTableInfo.isPctPossible()) {
                throw new AnalysisException(String.format("Unable to find a suitable base table for partitioning,"
                        + " the fail reason is %s", relatedTableInfo.getFailReason()));
            }
            MTMVRelatedTableIf mtmvBaseRealtedTable = MTMVUtil.getRelatedTable(relatedTableInfo.getTableInfo());
            Set<String> partitionColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            try {
                partitionColumnNames.addAll(mtmvBaseRealtedTable.getPartitionColumnNames());
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            if (!partitionColumnNames.contains(relatedTableInfo.getColumn())) {
                throw new AnalysisException("error related column: " + relatedTableInfo.getColumn());
            }
            if (!(mtmvBaseRealtedTable instanceof HMSExternalTable)
                    && partitionColumnNames.size() != 1) {
                throw new AnalysisException("only hms table support multi column partition.");
            }
            return relatedTableInfo;
        } finally {
            // after operate, roll back the disable rules
            sessionVariable.setDisableNereidsRules(String.join(",", tempDisableRules));
            cascadesContext.getStatementContext().invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
        }
    }

    private static List<Expr> convertToLegacyArguments(List<Expression> children) {
        return children.stream().map(MTMVPartitionDefinition::convertToLegacyRecursion).collect(Collectors.toList());
    }

    private static Expr convertToLegacyRecursion(Expression expression) {
        if (expression instanceof Slot) {
            return new SlotRef(null, ((Slot) expression).getName());
        } else if (expression instanceof Literal) {
            return new StringLiteral(((Literal) expression).getStringValue());
        } else if (expression instanceof Cast) {
            // mv partition roll up only need the slot in cast
            return convertToLegacyRecursion(((Cast) expression).child());
        } else {
            throw new AnalysisException("unsupported argument " + expression.toString());
        }
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

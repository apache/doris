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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
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
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
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
     * @return MTMVPartitionInfo
     */
    public MTMVPartitionInfo analyzeAndTransferToMTMVPartitionInfo(NereidsPlanner planner) {
        MTMVPartitionInfo mtmvPartitionInfo = new MTMVPartitionInfo(partitionType);
        if (this.partitionType == MTMVPartitionType.SELF_MANAGE) {
            return mtmvPartitionInfo;
        }
        String partitionColName;
        String timeUnit;
        if (this.partitionType == MTMVPartitionType.EXPR) {
            if (functionCallExpression instanceof UnboundFunction && PARTITION_BY_FUNCTION_NAME
                    .equalsIgnoreCase(((UnboundFunction) functionCallExpression).getName())) {
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
        fillPctInfos(planner, partitionColName, timeUnit, mtmvPartitionInfo);
        if (this.partitionType == MTMVPartitionType.EXPR) {
            try {
                MTMVPartitionExprFactory.getExprService(mtmvPartitionInfo.getExpr()).analyze(mtmvPartitionInfo);
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
        return mtmvPartitionInfo;
    }

    // Should use rewritten plan without view and subQuery to get related partition table
    private void fillPctInfos(NereidsPlanner planner, String partitionColName,
            String timeUnit, MTMVPartitionInfo mtmvPartitionInfo) {
        CascadesContext cascadesContext = planner.getCascadesContext();
        RelatedTableInfo relatedTableInfo = MaterializedViewUtils
                .getRelatedTableInfos(partitionColName, timeUnit, planner.getRewrittenPlan(), cascadesContext);
        if (!relatedTableInfo.isPctPossible()) {
            throw new AnalysisException(String.format("Unable to find a suitable base table for partitioning,"
                    + " the fail reason is %s", relatedTableInfo.getFailReason()));
        }
        List<RelatedTableColumnInfo> tableColumnInfos = relatedTableInfo.getTableColumnInfos();
        List<BaseColInfo> pctInfos = Lists.newArrayList();
        for (RelatedTableColumnInfo tableColumnInfo : tableColumnInfos) {
            String columnStr = tableColumnInfo.getColumnStr();
            BaseTableInfo tableInfo = tableColumnInfo.getTableInfo();
            BaseColInfo baseColInfo = new BaseColInfo(columnStr, tableInfo);
            pctInfos.add(baseColInfo);
            Optional<Expression> partitionExpression = tableColumnInfo.getPartitionExpression();
            if (partitionExpression.isPresent() && partitionExpression.get().getExpressionName()
                    .equalsIgnoreCase(PARTITION_BY_FUNCTION_NAME)) {
                DateTrunc dateTrunc = (DateTrunc) partitionExpression.get();
                mtmvPartitionInfo.setExpr(new FunctionCallExpr(dateTrunc.getName(),
                        new FunctionParams(convertToLegacyArguments(dateTrunc.children()))));
                mtmvPartitionInfo.setPartitionType(MTMVPartitionType.EXPR);
                this.partitionType = MTMVPartitionType.EXPR;
            }

        }
        if (pctInfos.isEmpty()) {
            throw new AnalysisException(
                    "Unable to find a suitable base table for partitioning,the fail reason is pctInfosSet.size() is 0");
        }
        MTMVRelatedTableIf relatedTable = MTMVUtil.getRelatedTable(pctInfos.get(0).getTableInfo());
        PartitionType relatedTablePartitionType = relatedTable.getPartitionType(
                MvccUtil.getSnapshotFromContext(relatedTable));
        if (pctInfos.size() > 1) {
            // check all partition type of pct table is same
            for (BaseColInfo baseColInfo : pctInfos) {
                MTMVRelatedTableIf pctTable = MTMVUtil.getRelatedTable(baseColInfo.getTableInfo());
                PartitionType partitionType = pctTable.getPartitionType(
                        MvccUtil.getSnapshotFromContext(pctTable));
                if (!partitionType.equals(relatedTablePartitionType)) {
                    throw new AnalysisException("partition type of multi pctTables must be same, pctInfos:" + pctInfos);
                }
            }
        }
        if (relatedTablePartitionType.equals(PartitionType.RANGE)) {
            for (BaseColInfo baseColInfo : pctInfos) {
                MTMVRelatedTableIf pctTable = MTMVUtil.getRelatedTable(baseColInfo.getTableInfo());
                List<Column> partitionColumns = pctTable.getPartitionColumns(MvccUtil.getSnapshotFromContext(pctTable));
                if (partitionColumns.size() != 1) {
                    throw new AnalysisException(String.format(
                            "only List PartitionType support multi columns partition, "
                                    + "but [%s] have [%s] partitionColumns.",
                            baseColInfo.getTableInfo(), partitionColumns.size()));
                }
            }
        }
        // for compatible
        mtmvPartitionInfo.setRelatedCol(pctInfos.get(0).getColName());
        mtmvPartitionInfo.setRelatedTable(pctInfos.get(0).getTableInfo());
        mtmvPartitionInfo.setPctInfos(pctInfos);
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

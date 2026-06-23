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

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
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
import org.apache.doris.nereids.rules.exploration.mv.PartitionIncrementMaintainer;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo;
import org.apache.doris.nereids.rules.exploration.mv.RelatedTableInfo.RelatedTableColumnInfo;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.literal.Interval;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
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
        boolean allowFallbackPartitionExpr = true;
        if (this.partitionType == MTMVPartitionType.EXPR) {
            if (functionCallExpression instanceof UnboundFunction && PARTITION_BY_FUNCTION_NAME
                    .equalsIgnoreCase(((UnboundFunction) functionCallExpression).getName())) {
                Expression dateTruncArg = functionCallExpression.getArgument(0);
                allowFallbackPartitionExpr = !isSlotLikePartitionArg(dateTruncArg);
                timeUnit = functionCallExpression.getArguments().get(1).isLiteral()
                        ? ((Literal) functionCallExpression.getArgument(1)).getStringValue() : null;
                ResolvedPartitionColumn resolved = resolvePartitionColumnForDateTrunc(planner, dateTruncArg, timeUnit);
                partitionColName = resolved.partitionColName;
                timeUnit = resolved.timeUnit;
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
        if (this.partitionType == MTMVPartitionType.EXPR && mtmvPartitionInfo.getExpr() == null
                && allowFallbackPartitionExpr) {
            List<Pair<Integer, Expr>> paramPairs =
                    convertDateTruncToLegacyArguments(((UnboundFunction) functionCallExpression).getArguments());
            List<Expr> params = paramPairs.stream()
                    .sorted(Comparator.comparingInt(Pair::key))
                    .map(Pair::value)
                    .collect(Collectors.toList());
            mtmvPartitionInfo.setExpr(new FunctionCallExpr(PARTITION_BY_FUNCTION_NAME, params, false));
            mtmvPartitionInfo.setPartitionType(MTMVPartitionType.EXPR);
        }
        if (this.partitionType == MTMVPartitionType.EXPR && mtmvPartitionInfo.getExpr() == null) {
            throw new AnalysisException("failed to derive mtmv partition expression from SELECT output. "
                    + "Please expose the full partition expression in SELECT with an alias");
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

    private static final class ResolvedPartitionColumn {
        private final String partitionColName;
        private final String timeUnit;

        private ResolvedPartitionColumn(String partitionColName, String timeUnit) {
            this.partitionColName = partitionColName;
            this.timeUnit = timeUnit;
        }
    }

    /**
     * Resolve an MV output column name for MTMV partition tracking.
     *
     * The downstream create/reanalysis expects {@link MTMVPartitionInfo#getPartitionCol()} to be a real MV output
     * column name, so for raw nested forms like:
     *   PARTITION BY (date_trunc(date_add(k2, INTERVAL 3 HOUR), 'day'))
     * we must find a matching SELECT column and use that name as partitionCol.
     */
    private static ResolvedPartitionColumn resolvePartitionColumnForDateTrunc(NereidsPlanner planner,
            Expression dateTruncArg, String timeUnit) {
        if (isSlotLikePartitionArg(dateTruncArg)) {
            return new ResolvedPartitionColumn(extractSlotName(dateTruncArg), timeUnit);
        }
        CascadesContext cascadesContext = planner.getCascadesContext();
        Plan planWithoutSink = PartitionIncrementMaintainer.removeSink(planner.getRewrittenPlan());
        Optional<DateTruncDateAddSubSignature> fullSig =
                extractDateTruncDateAddSubSignature(PARTITION_BY_FUNCTION_NAME, dateTruncArg, timeUnit);
        if (fullSig.isPresent()) {
            Optional<String> matchedColumn = matchOutputColumnByDateTruncSignature(planWithoutSink, cascadesContext,
                    fullSig.get());
            if (matchedColumn.isPresent()) {
                return new ResolvedPartitionColumn(matchedColumn.get(), null);
            }
        }

        Optional<HourOffsetSignature> offsetSig = extractHourOffsetSignature(dateTruncArg);
        if (offsetSig.isPresent()) {
            Optional<String> matchedColumn = matchOutputColumnByHourOffsetSignature(planWithoutSink, cascadesContext,
                    offsetSig.get());
            if (matchedColumn.isPresent()) {
                return new ResolvedPartitionColumn(matchedColumn.get(), timeUnit);
            }
        }
        throw new AnalysisException("partition expression must reference a SELECT output column. "
                + "Please expose the partition expression (or its date_add/date_sub argument) in SELECT with an alias");
    }

    private static Optional<String> matchOutputColumnByDateTruncSignature(Plan planWithoutSink,
            CascadesContext cascadesContext, DateTruncDateAddSubSignature signature) {
        ExpressionNormalization normalization = new ExpressionNormalization();
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(cascadesContext);
        List<String> matched = new ArrayList<>();
        for (Slot outputSlot : planWithoutSink.getOutput()) {
            Expression lineage = ExpressionUtils.shuttleExpressionWithLineage(outputSlot, planWithoutSink);
            lineage = normalization.rewrite(lineage, rewriteContext);
            Optional<DateTruncDateAddSubSignature> sig = extractDateTruncDateAddSubSignature(lineage);
            if (sig.isPresent() && sig.get().equals(signature)) {
                matched.add(outputSlot.getName());
            }
        }
        if (matched.isEmpty()) {
            return Optional.empty();
        }
        if (matched.size() != 1) {
            throw new AnalysisException("partition expression matches multiple SELECT columns: " + matched
                    + ", please use an explicit alias in PARTITION BY");
        }
        return Optional.of(matched.get(0));
    }

    private static Optional<String> matchOutputColumnByHourOffsetSignature(Plan planWithoutSink,
            CascadesContext cascadesContext, HourOffsetSignature signature) {
        ExpressionNormalization normalization = new ExpressionNormalization();
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(cascadesContext);
        List<String> matched = new ArrayList<>();
        for (Slot outputSlot : planWithoutSink.getOutput()) {
            Expression lineage = ExpressionUtils.shuttleExpressionWithLineage(outputSlot, planWithoutSink);
            lineage = normalization.rewrite(lineage, rewriteContext);
            Optional<HourOffsetSignature> sig = extractHourOffsetSignatureFromAny(lineage);
            if (sig.isPresent() && sig.get().equals(signature)) {
                matched.add(outputSlot.getName());
            }
        }
        if (matched.isEmpty()) {
            return Optional.empty();
        }
        if (matched.size() != 1) {
            throw new AnalysisException("partition expression matches multiple SELECT columns: " + matched
                    + ", please use an explicit alias in PARTITION BY");
        }
        return Optional.of(matched.get(0));
    }

    private static boolean isSlotLikePartitionArg(Expression expression) {
        if (expression instanceof UnboundSlot || expression instanceof SlotReference) {
            return true;
        } else if (expression instanceof Slot) {
            return true;
        } else if (expression instanceof Cast) {
            return isSlotLikePartitionArg(((Cast) expression).child());
        } else {
            return false;
        }
    }

    private static final class HourOffsetSignature {
        private final String baseSlotName;
        private final long offsetHours;

        private HourOffsetSignature(String baseSlotName, long offsetHours) {
            this.baseSlotName = baseSlotName;
            this.offsetHours = offsetHours;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof HourOffsetSignature)) {
                return false;
            }
            HourOffsetSignature that = (HourOffsetSignature) o;
            return baseSlotName.equalsIgnoreCase(that.baseSlotName) && offsetHours == that.offsetHours;
        }

        @Override
        public int hashCode() {
            return baseSlotName.toLowerCase().hashCode() * 31 + Long.hashCode(offsetHours);
        }
    }

    private static Optional<HourOffsetSignature> extractHourOffsetSignatureFromAny(Expression expression) {
        Optional<HourOffsetSignature> direct = extractHourOffsetSignature(expression);
        if (direct.isPresent()) {
            return direct;
        }
        if (expression instanceof DateTrunc && expression.arity() >= 1) {
            return extractHourOffsetSignature(expression.child(0));
        }
        if (expression instanceof UnboundFunction
                && PARTITION_BY_FUNCTION_NAME.equalsIgnoreCase(((UnboundFunction) expression).getName())
                && expression.arity() >= 1) {
            return extractHourOffsetSignature(expression.child(0));
        }
        return Optional.empty();
    }

    private static Optional<HourOffsetSignature> extractHourOffsetSignature(Expression expression) {
        if (expression instanceof Cast) {
            return extractHourOffsetSignature(((Cast) expression).child());
        }
        if (expression instanceof HoursAdd) {
            String slotName = extractSlotName(expression.child(0));
            if (slotName == null) {
                return Optional.empty();
            }
            return Optional.of(new HourOffsetSignature(slotName, extractIntervalHours(expression.child(1))));
        } else if (expression instanceof HoursSub) {
            String slotName = extractSlotName(expression.child(0));
            if (slotName == null) {
                return Optional.empty();
            }
            return Optional.of(new HourOffsetSignature(slotName, -extractIntervalHours(expression.child(1))));
        } else if (expression instanceof UnboundFunction) {
            String name = ((UnboundFunction) expression).getName().toLowerCase();
            if ("hours_add".equals(name) || "date_add".equals(name)) {
                String slotName = extractSlotName(expression.child(0));
                if (slotName == null) {
                    return Optional.empty();
                }
                return Optional.of(new HourOffsetSignature(slotName, extractIntervalHours(expression.child(1))));
            } else if ("hours_sub".equals(name) || "date_sub".equals(name)) {
                String slotName = extractSlotName(expression.child(0));
                if (slotName == null) {
                    return Optional.empty();
                }
                return Optional.of(new HourOffsetSignature(slotName, -extractIntervalHours(expression.child(1))));
            }
        }
        return Optional.empty();
    }

    private static long extractIntervalHours(Expression offsetExpression) {
        if (offsetExpression instanceof Literal) {
            Object v = ((Literal) offsetExpression).getValue();
            if (!(v instanceof Number)) {
                throw new AnalysisException("date arithmetic offset should be numeric literal: " + offsetExpression);
            }
            return ((Number) v).longValue();
        } else if (offsetExpression instanceof Interval) {
            Interval interval = (Interval) offsetExpression;
            if (interval.timeUnit() != Interval.TimeUnit.HOUR) {
                throw new AnalysisException("only HOUR unit is supported in date_add/date_sub for mtmv partition");
            }
            return extractIntervalHours(interval.value());
        } else if (offsetExpression instanceof Cast) {
            return extractIntervalHours(((Cast) offsetExpression).child());
        } else {
            throw new AnalysisException("date arithmetic offset should be literal: " + offsetExpression);
        }
    }

    private static final class DateTruncDateAddSubSignature {
        private final String baseSlotName;
        private final long offsetHours;
        private final String timeUnit;

        private DateTruncDateAddSubSignature(String baseSlotName, long offsetHours, String timeUnit) {
            this.baseSlotName = baseSlotName;
            this.offsetHours = offsetHours;
            this.timeUnit = timeUnit;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DateTruncDateAddSubSignature)) {
                return false;
            }
            DateTruncDateAddSubSignature that = (DateTruncDateAddSubSignature) o;
            return baseSlotName.equalsIgnoreCase(that.baseSlotName)
                    && offsetHours == that.offsetHours
                    && timeUnit.equalsIgnoreCase(that.timeUnit);
        }

        @Override
        public int hashCode() {
            int h = baseSlotName.toLowerCase().hashCode();
            h = 31 * h + Long.hashCode(offsetHours);
            h = 31 * h + timeUnit.toLowerCase().hashCode();
            return h;
        }
    }

    private static Optional<DateTruncDateAddSubSignature> extractDateTruncDateAddSubSignature(Expression expression) {
        if (expression instanceof DateTrunc) {
            Expression arg0 = expression.child(0);
            Expression arg1 = expression.child(1);
            if (!(arg1 instanceof Literal)) {
                return Optional.empty();
            }
            String unit = ((Literal) arg1).getStringValue();
            Optional<HourOffsetSignature> offset = extractHourOffsetSignature(arg0);
            if (!offset.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new DateTruncDateAddSubSignature(offset.get().baseSlotName, offset.get().offsetHours,
                    unit));
        } else if (expression instanceof UnboundFunction
                && PARTITION_BY_FUNCTION_NAME.equalsIgnoreCase(((UnboundFunction) expression).getName())
                && expression.arity() == 2) {
            Expression arg0 = expression.child(0);
            Expression arg1 = expression.child(1);
            if (!(arg1 instanceof Literal)) {
                return Optional.empty();
            }
            String unit = ((Literal) arg1).getStringValue();
            Optional<HourOffsetSignature> offset = extractHourOffsetSignature(arg0);
            if (!offset.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new DateTruncDateAddSubSignature(offset.get().baseSlotName, offset.get().offsetHours,
                    unit));
        }
        return Optional.empty();
    }

    private static Optional<DateTruncDateAddSubSignature> extractDateTruncDateAddSubSignature(String funcName,
            Expression dateTruncArg, String timeUnit) {
        if (!PARTITION_BY_FUNCTION_NAME.equalsIgnoreCase(funcName) || timeUnit == null) {
            return Optional.empty();
        }
        Optional<HourOffsetSignature> offset = extractHourOffsetSignature(dateTruncArg);
        if (!offset.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new DateTruncDateAddSubSignature(offset.get().baseSlotName, offset.get().offsetHours,
                timeUnit));
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
        boolean exprSetFromLineage = false;
        for (RelatedTableColumnInfo tableColumnInfo : tableColumnInfos) {
            String columnStr = tableColumnInfo.getColumnStr();
            BaseTableInfo tableInfo = tableColumnInfo.getTableInfo();
            BaseColInfo baseColInfo = new BaseColInfo(columnStr, tableInfo);
            pctInfos.add(baseColInfo);
            Optional<Expression> partitionExpression = tableColumnInfo.getPartitionExpression();
            if (partitionExpression.isPresent() && partitionExpression.get().getExpressionName()
                    .equalsIgnoreCase(PARTITION_BY_FUNCTION_NAME)) {
                DateTrunc dateTrunc = (DateTrunc) partitionExpression.get();
                List<Pair<Integer, Expr>> paramPairs = convertDateTruncToLegacyArguments(dateTrunc.children());
                List<Expr> params = paramPairs.stream()
                        .sorted(Comparator.comparingInt(Pair::key))
                        .map(Pair::value)
                        .collect(Collectors.toList());
                FunctionCallExpr legacyExpr = new FunctionCallExpr(dateTrunc.getName(), params, false);
                if (!exprSetFromLineage) {
                    // Prefer lineage-derived partition expression so alias-form PARTITION BY
                    // can be resolved to the real base-table expression (including hour offsets / casts).
                    mtmvPartitionInfo.setExpr(legacyExpr);
                    mtmvPartitionInfo.setPartitionType(MTMVPartitionType.EXPR);
                    this.partitionType = MTMVPartitionType.EXPR;
                    exprSetFromLineage = true;
                }
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

    private static List<Pair<Integer, Expr>> convertDateTruncToLegacyArguments(List<Expression> children) {
        return children.stream().map(MTMVPartitionDefinition::convertToLegacyRecursion).collect(Collectors.toList());
    }

    private static Pair<Integer, Expr> convertToLegacyRecursion(Expression expression) {
        if (expression instanceof UnboundSlot) {
            return Pair.of(1, new SlotRef(null, ((UnboundSlot) expression).getName()));
        } else if (expression instanceof Slot) {
            return Pair.of(1, new SlotRef(null, ((Slot) expression).getName()));
        } else if (expression instanceof Cast) {
            Pair<Integer, Expr> child = convertToLegacyRecursion(((Cast) expression).child());
            Type castTargetType = ((Cast) expression).getDataType().toCatalogDataType();
            return Pair.of(child.key(), new CastExpr(castTargetType, child.value(), expression.nullable()));
        } else if (expression instanceof HoursAdd) {
            return Pair.of(1, convertDateAddSubToLegacy((HoursAdd) expression, "date_add"));
        } else if (expression instanceof HoursSub) {
            return Pair.of(1, convertDateAddSubToLegacy((HoursSub) expression, "date_sub"));
        } else if (expression instanceof UnboundFunction) {
            String name = ((UnboundFunction) expression).getName().toLowerCase();
            if ("hours_add".equals(name) || "date_add".equals(name)) {
                return Pair.of(1, convertDateAddSubToLegacy(expression, "date_add"));
            } else if ("hours_sub".equals(name) || "date_sub".equals(name)) {
                return Pair.of(1, convertDateAddSubToLegacy(expression, "date_sub"));
            } else {
                throw new AnalysisException("unsupported argument " + expression.toString());
            }
        } else if (expression instanceof Literal) {
            return Pair.of(2, new StringLiteral(((Literal) expression).getStringValue()));
        } else {
            throw new AnalysisException("unsupported argument " + expression.toString());
        }
    }

    private static TimestampArithmeticExpr convertDateAddSubToLegacy(Expression expression, String funcName) {
        Pair<Integer, Expr> timeExprPair = convertToLegacyRecursion(expression.child(0));
        if (timeExprPair.key() != 1) {
            throw new AnalysisException("unsupported date arithmetic argument " + expression.toString());
        }
        Expr amountExpr;
        Expression offsetExpression = expression.child(1);
        if (offsetExpression instanceof Literal) {
            amountExpr = ((Literal) offsetExpression).toLegacyLiteral();
        } else if (offsetExpression instanceof Interval) {
            Interval interval = (Interval) offsetExpression;
            if (interval.timeUnit() != Interval.TimeUnit.HOUR) {
                throw new AnalysisException(
                        "only HOUR unit is supported in date_add/date_sub for mtmv partition: " + expression);
            }
            if (!(interval.value() instanceof Literal)) {
                throw new AnalysisException("date arithmetic offset should be literal " + expression.toString());
            }
            amountExpr = ((Literal) interval.value()).toLegacyLiteral();
        } else {
            throw new AnalysisException("date arithmetic offset should be literal " + expression.toString());
        }
        return new TimestampArithmeticExpr(funcName, timeExprPair.value(), amountExpr, "HOUR");
    }

    private static String extractSlotName(Expression expression) {
        if (expression instanceof UnboundSlot) {
            return ((UnboundSlot) expression).getName();
        } else if (expression instanceof Slot) {
            return ((Slot) expression).getName();
        } else if (expression instanceof Cast) {
            return extractSlotName(((Cast) expression).child());
        } else if (expression instanceof UnboundFunction) {
            if (expression.arity() <= 0) {
                return null;
            }
            return extractSlotName(expression.child(0));
        } else if (expression instanceof HoursAdd || expression instanceof HoursSub) {
            return extractSlotName(expression.child(0));
        } else {
            return null;
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

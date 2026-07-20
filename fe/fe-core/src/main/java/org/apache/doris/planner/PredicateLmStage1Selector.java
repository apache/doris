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

package org.apache.doris.planner;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Selects cheap and selective predicate columns as stage1 columns for multi-stage predicate LM.
 */
public class PredicateLmStage1Selector {
    private static final int MAX_STAGE1_COLUMNS = 2;
    private static final double DEFAULT_UNKNOWN_SELECTIVITY = 0.5;
    private static final double MIN_SELECTIVITY = 0.001;
    private static final double MAX_SELECTIVITY = 1.0;
    private static final double TARGET_STAGE1_SELECTIVITY_FOR_EARLY_STOP = 0.03;
    private static final double MAX_STAGE1_SELECTIVITY_FOR_SAVED_BYTES_GATE = 0.10;
    private static final long MIN_ESTIMATED_SAVED_BYTES_FOR_AUTO_SELECT = 1L << 30;
    private static final double CHEAP_STAGE1_EVAL_COST_THRESHOLD = 2.0;

    private final OlapScanNode scanNode;
    private final OlapTable table;
    private final SessionVariable sessionVariable;
    private final ConnectContext connectContext;
    private final Function<String, ColumnStatistic> columnStatisticProvider;

    public PredicateLmStage1Selector(OlapScanNode scanNode, SessionVariable sessionVariable) {
        this(scanNode, sessionVariable, null);
    }

    PredicateLmStage1Selector(OlapScanNode scanNode, SessionVariable sessionVariable,
            Function<String, ColumnStatistic> columnStatisticProvider) {
        this.scanNode = scanNode;
        this.table = scanNode.getOlapTable();
        this.sessionVariable = sessionVariable;
        this.connectContext = ConnectContext.get();
        this.columnStatisticProvider = columnStatisticProvider;
    }

    public SelectionResult select() {
        if (sessionVariable == null || !sessionVariable.enableMultiStagePredicateLm) {
            return SelectionResult.rejected(SelectionReason.DISABLED);
        }
        if (scanNode.getConjuncts().size() < 2) {
            return SelectionResult.rejected(SelectionReason.TOO_FEW_CONJUNCTS);
        }

        List<Column> schema = getSelectedSchema();
        Map<String, Integer> columnNameToId = buildColumnIdMap(schema);
        Map<String, CandidateInfo> candidateInfos = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Expr conjunct : scanNode.getConjuncts()) {
            Optional<PredicateInfo> info = PredicateInfo.create(conjunct);
            if (!info.isPresent()) {
                continue;
            }

            SlotRef slot = info.get().slotRef;
            Column column = slot.getColumn();
            if (column == null || column.getName() == null || !columnNameToId.containsKey(column.getName())) {
                continue;
            }
            CandidateInfo candidateInfo = candidateInfos.computeIfAbsent(column.getName(),
                    columnName -> new CandidateInfo(columnName, columnNameToId.get(columnName), column));
            candidateInfo.predicates.add(info.get());
        }

        if (candidateInfos.size() < 2) {
            return SelectionResult.rejected(SelectionReason.TOO_FEW_CANDIDATES);
        }

        Map<String, PredicateLmCandidate> candidates = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (CandidateInfo candidateInfo : candidateInfos.values()) {
            PredicateLmCandidate candidate = new PredicateLmCandidate(candidateInfo.columnName,
                    candidateInfo.columnId, candidateInfo.column, getColumnStatistic(candidateInfo.columnName));
            candidate.predicates.addAll(candidateInfo.predicates);
            candidates.put(candidateInfo.columnName, candidate);
        }

        for (PredicateLmCandidate candidate : candidates.values()) {
            candidate.finish();
        }

        List<PredicateLmCandidate> selected = chooseStage1Candidates(candidates);
        if (selected.isEmpty()) {
            return SelectionResult.rejected(SelectionReason.NO_STAGE1_CANDIDATE);
        }
        if (!existsLateCandidate(candidates, selected)) {
            return SelectionResult.rejected(SelectionReason.NO_LATE_CANDIDATE);
        }

        double stage1Selectivity = combineSelectivity(selected);
        double latePredicateReadBytesPerRow = estimateLatePredicateReadBytesPerRow(candidates, selected);
        double estimatedSavedBytes = estimateSavedBytes(candidates, stage1Selectivity,
                latePredicateReadBytesPerRow);
        double minEstimatedSavedBytes = minEstimatedSavedBytes(latePredicateReadBytesPerRow);
        if (stage1Selectivity > maxStage1SelectivityForSavedBytesGate()) {
            return SelectionResult.rejected(SelectionReason.HIGH_SELECTIVITY, stage1Selectivity,
                    estimatedSavedBytes, minEstimatedSavedBytes);
        }
        if (estimatedSavedBytes < minEstimatedSavedBytes) {
            return SelectionResult.rejected(SelectionReason.LOW_SAVED_BYTES, stage1Selectivity,
                    estimatedSavedBytes, minEstimatedSavedBytes);
        }

        List<Integer> stage1ColumnIds = Lists.newArrayListWithExpectedSize(selected.size());
        selected.forEach(candidate -> stage1ColumnIds.add(candidate.columnId));
        List<String> stage1ColumnNames = selected.stream()
                .map(candidate -> candidate.columnName)
                .collect(Collectors.toList());
        return new SelectionResult(stage1ColumnIds, stage1ColumnNames, SelectionReason.SELECTED,
                stage1Selectivity, estimatedSavedBytes, minEstimatedSavedBytes);
    }

    private List<PredicateLmCandidate> chooseStage1Candidates(Map<String, PredicateLmCandidate> candidates) {
        List<PredicateLmCandidate> sorted = new ArrayList<>();
        for (PredicateLmCandidate candidate : candidates.values()) {
            if (candidate.eligibleStage1) {
                sorted.add(candidate);
            }
        }
        sorted.sort(Comparator.comparingDouble(PredicateLmCandidate::stage1Score).reversed());

        List<PredicateLmCandidate> selected = new ArrayList<>();
        double combinedSelectivity = 1.0;
        for (PredicateLmCandidate candidate : sorted) {
            if (selected.size() >= MAX_STAGE1_COLUMNS) {
                break;
            }
            selected.add(candidate);
            combinedSelectivity *= candidate.estimatedSelectivity;
            if (combinedSelectivity <= targetStage1SelectivityForEarlyStop()) {
                break;
            }
        }
        return selected;
    }

    private double targetStage1SelectivityForEarlyStop() {
        return Math.min(sessionVariable.predicateLmStage1SurvivalRatioThreshold,
                TARGET_STAGE1_SELECTIVITY_FOR_EARLY_STOP);
    }

    private double maxStage1SelectivityForSavedBytesGate() {
        return Math.min(sessionVariable.predicateLmStage1SurvivalRatioThreshold,
                MAX_STAGE1_SELECTIVITY_FOR_SAVED_BYTES_GATE);
    }

    private double minEstimatedSavedBytes(double latePredicateReadBytesPerRow) {
        double minScanRowsSavedBytes = sessionVariable.predicateLmMinScanRows * latePredicateReadBytesPerRow;
        return Math.max(minScanRowsSavedBytes, MIN_ESTIMATED_SAVED_BYTES_FOR_AUTO_SELECT);
    }

    private boolean existsLateCandidate(Map<String, PredicateLmCandidate> candidates,
            List<PredicateLmCandidate> selected) {
        Set<String> selectedColumnNames = new HashSet<>();
        selected.forEach(candidate -> selectedColumnNames.add(candidate.columnName));
        for (PredicateLmCandidate candidate : candidates.values()) {
            if (!selectedColumnNames.contains(candidate.columnName) && candidate.isLatePredicateCandidate()) {
                return true;
            }
        }
        return false;
    }

    private double estimateLatePredicateReadBytesPerRow(Map<String, PredicateLmCandidate> candidates,
            List<PredicateLmCandidate> selected) {
        Set<String> selectedColumnNames = new HashSet<>();
        selected.forEach(candidate -> selectedColumnNames.add(candidate.columnName));

        double latePredicateReadBytesPerRow = 0.0;
        for (PredicateLmCandidate candidate : candidates.values()) {
            if (!selectedColumnNames.contains(candidate.columnName)) {
                latePredicateReadBytesPerRow += candidate.readCost;
            }
        }
        if (latePredicateReadBytesPerRow <= 0) {
            return 0.0;
        }
        return latePredicateReadBytesPerRow;
    }

    private double estimateSavedBytes(Map<String, PredicateLmCandidate> candidates, double stage1Selectivity,
            double latePredicateReadBytesPerRow) {
        double estimatedInputRows = estimateInputRows(candidates);
        double estimatedRowsAvoided = estimatedInputRows * (1.0 - stage1Selectivity);
        return estimatedRowsAvoided * latePredicateReadBytesPerRow;
    }

    private double estimateInputRows(Map<String, PredicateLmCandidate> candidates) {
        long scanCardinality = scanNode.getCardinality();
        double estimatedInputRows = scanCardinality > 0 ? scanCardinality : 0.0;
        for (PredicateLmCandidate candidate : candidates.values()) {
            if (!candidate.stats.isUnKnown && candidate.stats.count > 0) {
                estimatedInputRows = Math.max(estimatedInputRows, candidate.stats.count);
            }
        }
        return estimatedInputRows;
    }

    private double combineSelectivity(List<PredicateLmCandidate> candidates) {
        double selectivity = 1.0;
        for (PredicateLmCandidate candidate : candidates) {
            selectivity *= candidate.estimatedSelectivity;
        }
        return clamp(selectivity, MIN_SELECTIVITY, MAX_SELECTIVITY);
    }

    private List<Column> getSelectedSchema() {
        long selectedIndexId = scanNode.getSelectedIndexId();
        return selectedIndexId == -1 ? table.getBaseSchema() : table.getSchemaByIndexId(selectedIndexId);
    }

    private Map<String, Integer> buildColumnIdMap(List<Column> schema) {
        Map<String, Integer> columnNameToId = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < schema.size(); i++) {
            columnNameToId.put(schema.get(i).getName(), i);
        }
        return columnNameToId;
    }

    private ColumnStatistic getColumnStatistic(String columnName) {
        if (columnStatisticProvider != null) {
            return columnStatisticProvider.apply(columnName);
        }
        DatabaseIf database = table.getDatabase();
        CatalogIf catalog = database == null ? null : database.getCatalog();
        long catalogId = catalog == null ? -1 : catalog.getId();
        long dbId = database == null ? -1 : database.getId();
        long indexId = scanNode.getSelectedIndexId() == table.getBaseIndexId() ? -1 : scanNode.getSelectedIndexId();
        return Env.getCurrentEnv().getStatisticsCache()
                .getColumnStatistics(catalogId, dbId, table.getId(), indexId, columnName, connectContext);
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static class CandidateInfo {
        private final String columnName;
        private final int columnId;
        private final Column column;
        private final List<PredicateInfo> predicates = new ArrayList<>();

        private CandidateInfo(String columnName, int columnId, Column column) {
            this.columnName = columnName;
            this.columnId = columnId;
            this.column = column;
        }
    }

    private static class PredicateLmCandidate {
        private final String columnName;
        private final int columnId;
        private final Column column;
        private final ColumnStatistic stats;
        private final List<PredicateInfo> predicates = new ArrayList<>();
        private double estimatedSelectivity = 1.0;
        private double evalCost = 1.0;
        private double readCost = 1.0;
        private boolean reliableSelectivity = false;
        private boolean eligibleStage1 = false;
        private double stage1Score = 0.0;

        private PredicateLmCandidate(String columnName, int columnId, Column column, ColumnStatistic stats) {
            this.columnName = columnName;
            this.columnId = columnId;
            this.column = column;
            this.stats = stats;
        }

        private void finish() {
            estimatedSelectivity = 1.0;
            evalCost = 0.0;
            reliableSelectivity = true;
            for (PredicateInfo predicate : predicates) {
                estimatedSelectivity *= predicate.estimateSelectivity(stats);
                evalCost += predicate.evalCost;
                reliableSelectivity &= predicate.hasReliableSelectivity(stats);
            }
            estimatedSelectivity = clamp(estimatedSelectivity, MIN_SELECTIVITY, MAX_SELECTIVITY);
            readCost = estimateReadCost(column.getType(), stats);
            eligibleStage1 = reliableSelectivity && evalCost <= CHEAP_STAGE1_EVAL_COST_THRESHOLD
                    && !hasExpensivePredicate();

            double filterPower = -Math.log(estimatedSelectivity);
            double costPenalty = Math.max(0.1, evalCost + 0.2 * readCost);
            stage1Score = filterPower / costPenalty;
        }

        private double stage1Score() {
            return stage1Score;
        }

        private boolean isLatePredicateCandidate() {
            return !predicates.isEmpty();
        }

        private boolean hasExpensivePredicate() {
            for (PredicateInfo predicate : predicates) {
                if (predicate.expensive) {
                    return true;
                }
            }
            return false;
        }

        private static double estimateReadCost(Type type, ColumnStatistic stats) {
            if (type.isStringType() || type.isJsonbType() || type.isVariantType()) {
                return Math.max(16.0, stats.isUnKnown ? 16.0 : stats.avgSizeByte);
            }
            if (type.isComplexType()) {
                return 32.0;
            }
            return Math.max(1.0, stats.isUnKnown ? 1.0 : stats.avgSizeByte);
        }
    }

    private static class PredicateInfo {
        private final Expr expr;
        private final SlotRef slotRef;
        private final PredicateKind kind;
        private final double evalCost;
        private final boolean expensive;
        private final boolean negated;

        private PredicateInfo(Expr expr, SlotRef slotRef, PredicateKind kind, double evalCost, boolean expensive,
                boolean negated) {
            this.expr = expr;
            this.slotRef = slotRef;
            this.kind = kind;
            this.evalCost = evalCost;
            this.expensive = expensive;
            this.negated = negated;
        }

        private static Optional<PredicateInfo> create(Expr expr) {
            if (expr instanceof BinaryPredicate) {
                return createBinaryPredicate((BinaryPredicate) expr);
            }
            if (expr instanceof InPredicate) {
                return createInPredicate((InPredicate) expr);
            }
            if (expr instanceof LikePredicate) {
                return createLikePredicate((LikePredicate) expr);
            }
            if (expr instanceof IsNullPredicate) {
                return createIsNullPredicate((IsNullPredicate) expr);
            }
            return Optional.empty();
        }

        private static Optional<PredicateInfo> createBinaryPredicate(BinaryPredicate predicate) {
            SlotRef leftSlot = predicate.getChild(0).unwrapSlotRef();
            SlotRef rightSlot = predicate.getChild(1).unwrapSlotRef();
            PredicateKind kind = binaryKind(predicate.getOp());
            if (kind == PredicateKind.UNKNOWN) {
                return Optional.empty();
            }
            if (leftSlot != null && rightSlot == null && predicate.getChild(1).isConstant()) {
                return Optional.of(new PredicateInfo(predicate, leftSlot, kind, 1.0, false, false));
            }
            if (rightSlot != null && leftSlot == null && predicate.getChild(0).isConstant()) {
                return Optional.of(new PredicateInfo(predicate, rightSlot, kind, 1.0, false, false));
            }
            return Optional.empty();
        }

        private static Optional<PredicateInfo> createInPredicate(InPredicate predicate) {
            SlotRef slot = predicate.getChild(0).unwrapSlotRef();
            if (slot == null || predicate.isNotIn() || !predicate.isLiteralChildren()) {
                return Optional.empty();
            }
            double evalCost = 1.0 + Math.min(1.0, predicate.getInElementNum() / 32.0);
            return Optional.of(new PredicateInfo(predicate, slot, PredicateKind.IN, evalCost, false, false));
        }

        private static Optional<PredicateInfo> createLikePredicate(LikePredicate predicate) {
            SlotRef slot = predicate.getChild(0).unwrapSlotRef();
            if (slot == null || !predicate.getChild(1).isConstant()) {
                return Optional.empty();
            }
            return Optional.of(new PredicateInfo(predicate, slot, PredicateKind.LIKE, 8.0, true, false));
        }

        private static Optional<PredicateInfo> createIsNullPredicate(IsNullPredicate predicate) {
            SlotRef slot = predicate.getChild(0).unwrapSlotRef();
            if (slot == null) {
                return Optional.empty();
            }
            return Optional.of(new PredicateInfo(predicate, slot, PredicateKind.IS_NULL, 1.0, false,
                    predicate.isNotNull()));
        }

        private static PredicateKind binaryKind(BinaryPredicate.Operator op) {
            switch (op) {
                case EQ:
                case EQ_FOR_NULL:
                    return PredicateKind.EQ;
                case LT:
                case LE:
                case GT:
                case GE:
                    return PredicateKind.RANGE;
                default:
                    return PredicateKind.UNKNOWN;
            }
        }

        private double estimateSelectivity(ColumnStatistic stats) {
            if (stats.isUnKnown) {
                return DEFAULT_UNKNOWN_SELECTIVITY;
            }
            switch (kind) {
                case EQ:
                    return stats.ndv > 0 ? clamp(1.0 / stats.ndv, MIN_SELECTIVITY, MAX_SELECTIVITY)
                            : DEFAULT_UNKNOWN_SELECTIVITY;
                case IN:
                    int inElements = ((InPredicate) expr).getInElementNum();
                    return stats.ndv > 0 ? clamp(inElements / stats.ndv, MIN_SELECTIVITY, MAX_SELECTIVITY)
                            : DEFAULT_UNKNOWN_SELECTIVITY;
                case RANGE:
                    return 0.333;
                case IS_NULL:
                    if (stats.count <= 0) {
                        return DEFAULT_UNKNOWN_SELECTIVITY;
                    }
                    double nullRatio = clamp(stats.numNulls / stats.count, MIN_SELECTIVITY, MAX_SELECTIVITY);
                    return negated ? clamp(1.0 - nullRatio, MIN_SELECTIVITY, MAX_SELECTIVITY) : nullRatio;
                case LIKE:
                    return 0.1;
                default:
                    return DEFAULT_UNKNOWN_SELECTIVITY;
            }
        }

        private boolean hasReliableSelectivity(ColumnStatistic stats) {
            if (stats.isUnKnown) {
                return false;
            }
            switch (kind) {
                case EQ:
                    return stats.ndv > 0;
                case IN:
                    return stats.ndv > 0;
                case IS_NULL:
                    return stats.count > 0;
                case RANGE:
                case LIKE:
                default:
                    return false;
            }
        }
    }

    private enum PredicateKind {
        EQ,
        IN,
        RANGE,
        IS_NULL,
        LIKE,
        UNKNOWN
    }

    public enum SelectionReason {
        SELECTED,
        DISABLED,
        TOO_FEW_CONJUNCTS,
        TOO_FEW_CANDIDATES,
        NO_STAGE1_CANDIDATE,
        NO_LATE_CANDIDATE,
        HIGH_SELECTIVITY,
        LOW_SAVED_BYTES
    }

    public static class SelectionResult {
        private final List<Integer> stage1ColumnIds;
        private final List<String> stage1ColumnNames;
        private final SelectionReason reason;
        private final double estimatedStage1Selectivity;
        private final double estimatedSavedBytes;
        private final double minEstimatedSavedBytes;

        private SelectionResult(List<Integer> stage1ColumnIds, List<String> stage1ColumnNames,
                SelectionReason reason, double estimatedStage1Selectivity, double estimatedSavedBytes,
                double minEstimatedSavedBytes) {
            this.stage1ColumnIds = stage1ColumnIds;
            this.stage1ColumnNames = stage1ColumnNames;
            this.reason = reason;
            this.estimatedStage1Selectivity = estimatedStage1Selectivity;
            this.estimatedSavedBytes = estimatedSavedBytes;
            this.minEstimatedSavedBytes = minEstimatedSavedBytes;
        }

        public static SelectionResult rejected(SelectionReason reason) {
            return rejected(reason, 1.0, 0.0, 0.0);
        }

        public static SelectionResult rejected(SelectionReason reason,
                double estimatedStage1Selectivity, double estimatedSavedBytes,
                double minEstimatedSavedBytes) {
            return new SelectionResult(Collections.emptyList(), Collections.emptyList(), reason,
                    estimatedStage1Selectivity, estimatedSavedBytes, minEstimatedSavedBytes);
        }

        public boolean isEmpty() {
            return stage1ColumnIds.isEmpty();
        }

        public List<Integer> getStage1ColumnIds() {
            return stage1ColumnIds;
        }

        public List<String> getStage1ColumnNames() {
            return stage1ColumnNames;
        }

        public SelectionReason getReason() {
            return reason;
        }

        public double getEstimatedStage1Selectivity() {
            return estimatedStage1Selectivity;
        }

        public double getEstimatedSavedBytes() {
            return estimatedSavedBytes;
        }

        public double getMinEstimatedSavedBytes() {
            return minEstimatedSavedBytes;
        }
    }
}

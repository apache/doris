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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorToThriftConverter;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.LiteralExprUtils;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.PointQueryFixedKeyConstraint;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public class ShortCircuitQueryContext {
    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    public final Planner planner;
    public final ByteString serializedDescTable;
    public final ByteString serializedOutputExpr;
    public final ByteString serializedQueryOptions;

    // For prepared statement cached structure,
    // there are some pre-calculated structure in Backend TabletFetch service
    // using this ID to find for this prepared statement
    public final UUID cacheID;

    public final int schemaVersion;
    public final OlapTable tbl;
    public final String tableName;
    private final long fileCacheQueryLimitBytes;
    private final long partitionTopologyVersion;

    public final OlapScanNode scanNode;
    public final Queriable analzyedQuery;
    private final PointQueryKeyTemplate pointQueryKeyTemplate;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    Map<Integer, byte[]> serializedFields = Maps.newHashMap();

    List<Type> returnTypes = null;

    public byte[] getSerializedField(int idx) {
        return serializedFields.getOrDefault(idx, null);
    }

    public void addSerializedField(int idx, byte[] serializedField) {
        serializedFields.put(idx, serializedField);
    }

    List<Type> getReturnTypes() {
        if (returnTypes == null) {
            returnTypes = analzyedQuery.getResultExprs()
                    .stream().map(e -> e.getType()).collect(Collectors.toList());
        }
        return returnTypes;
    }

    public ShortCircuitQueryContext(Planner planner, Queriable analzyedQuery) throws TException {
        this(planner, analzyedQuery,
                planner instanceof NereidsPlanner ? ((NereidsPlanner) planner).getStatementContext() : null);
    }

    public ShortCircuitQueryContext(Planner planner, Queriable analzyedQuery,
            StatementContext statementContext) throws TException {
        this.planner = planner;
        this.serializedDescTable = ByteString.copyFrom(
                new TSerializer().serialize(DescriptorToThriftConverter.toThrift(planner.getDescTable())));
        TQueryOptions options = planner.getQueryOptions() != null ? planner.getQueryOptions() : new TQueryOptions();
        this.fileCacheQueryLimitBytes = options.isSetFileCacheQueryLimitBytes()
                ? options.getFileCacheQueryLimitBytes()
                : -1;
        this.serializedQueryOptions = ByteString.copyFrom(
                new TSerializer().serialize(options));
        List<TExpr> exprs = new ArrayList<>();
        OlapScanNode olapScanNode = (OlapScanNode) planner.getScanNodes().get(0);
        List<Expr> pointQueryProjectList = olapScanNode.getPointQueryProjectList();
        if (pointQueryProjectList != null) {
            // project on scan node
            exprs.addAll(pointQueryProjectList.stream()
                    .map(ExprToThriftVisitor::treeToThrift).collect(Collectors.toList()));
        } else {
            // add output slots
            exprs.addAll(planner.getFragments().get(0).getOutputExprs().stream()
                    .map(ExprToThriftVisitor::treeToThrift).collect(Collectors.toList()));
        }
        TExprList exprList = new TExprList(exprs);
        serializedOutputExpr = ByteString.copyFrom(
                new TSerializer().serialize(exprList));
        this.cacheID = UUID.randomUUID();
        this.scanNode = olapScanNode;
        this.tbl = this.scanNode.getOlapTable();
        this.tableName = this.scanNode.getTableNameInPlan();
        this.schemaVersion = this.tbl.getBaseSchemaVersion();
        this.partitionTopologyVersion = this.tbl.getPartitionTopologyVersion();
        this.analzyedQuery = analzyedQuery;
        this.pointQueryKeyTemplate = PointQueryKeyTemplate.create(this.scanNode, statementContext);
    }

    @VisibleForTesting
    ShortCircuitQueryContext(OlapTable tbl, String tableName, int schemaVersion,
            long fileCacheQueryLimitBytes) {
        this.planner = null;
        this.serializedDescTable = ByteString.EMPTY;
        this.serializedOutputExpr = ByteString.EMPTY;
        this.serializedQueryOptions = ByteString.EMPTY;
        this.cacheID = UUID.randomUUID();
        this.tbl = tbl;
        this.tableName = tableName;
        this.schemaVersion = schemaVersion;
        this.fileCacheQueryLimitBytes = fileCacheQueryLimitBytes;
        this.partitionTopologyVersion = tbl.getPartitionTopologyVersion();
        this.scanNode = null;
        this.analzyedQuery = null;
        this.pointQueryKeyTemplate = PointQueryKeyTemplate.unsupported();
    }

    @VisibleForTesting
    ShortCircuitQueryContext(OlapScanNode scanNode, StatementContext statementContext) {
        this.planner = null;
        this.serializedDescTable = ByteString.EMPTY;
        this.serializedOutputExpr = ByteString.EMPTY;
        this.serializedQueryOptions = ByteString.EMPTY;
        this.cacheID = UUID.randomUUID();
        this.scanNode = scanNode;
        this.tbl = scanNode.getOlapTable();
        this.tableName = scanNode.getTableNameInPlan();
        this.schemaVersion = tbl.getBaseSchemaVersion();
        this.fileCacheQueryLimitBytes = -1;
        this.partitionTopologyVersion = tbl.getPartitionTopologyVersion();
        this.analzyedQuery = null;
        this.pointQueryKeyTemplate = PointQueryKeyTemplate.create(scanNode, statementContext);
    }

    public boolean isReusable(ConnectContext ctx) {
        return !this.tbl.isDropped
                && this.tbl.getBaseSchemaVersion() == this.schemaVersion
                && Objects.equals(this.tableName, this.tbl.getName())
                && this.fileCacheQueryLimitBytes == ctx.getSessionVariable().fileCacheQueryLimitBytes
                && this.tbl.getPartitionTopologyVersion() == this.partitionTopologyVersion;
    }

    public void sanitize() {
        Preconditions.checkNotNull(serializedDescTable);
        Preconditions.checkNotNull(serializedOutputExpr);
        Preconditions.checkNotNull(cacheID);
        Preconditions.checkNotNull(tbl);
        Preconditions.checkNotNull(tableName);
    }

    /** Build state owned by one execution without modifying the cached plan or scan conjuncts. */
    public PointQueryExecutionContext createPointQueryExecutionContext(StatementContext statementContext) {
        return pointQueryKeyTemplate.bind(statementContext);
    }

    private static class PointQueryKeyTemplate {
        private final List<Column> keyColumns;
        private final List<PlaceholderKeyBinding> placeholderBindings;
        private final List<List<Literal>> fixedConstraints;
        private final boolean complete;

        private PointQueryKeyTemplate(List<Column> keyColumns,
                List<PlaceholderKeyBinding> placeholderBindings,
                List<List<Literal>> fixedConstraints, boolean complete) {
            this.keyColumns = Collections.unmodifiableList(new ArrayList<>(keyColumns));
            this.placeholderBindings = Collections.unmodifiableList(new ArrayList<>(placeholderBindings));
            List<List<Literal>> immutableConstraints = new ArrayList<>(fixedConstraints.size());
            for (List<Literal> constraints : fixedConstraints) {
                immutableConstraints.add(Collections.unmodifiableList(new ArrayList<>(constraints)));
            }
            this.fixedConstraints = Collections.unmodifiableList(immutableConstraints);
            this.complete = complete;
        }

        private static PointQueryKeyTemplate unsupported() {
            return new PointQueryKeyTemplate(Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyList(), false);
        }

        private static PointQueryKeyTemplate create(OlapScanNode scanNode, StatementContext statementContext) {
            if (statementContext == null) {
                return unsupported();
            }
            List<Column> keyColumns = scanNode.getOlapTable().getBaseSchemaKeyColumns();
            if (keyColumns.isEmpty()) {
                return new PointQueryKeyTemplate(keyColumns, Collections.emptyList(),
                        Collections.emptyList(), true);
            }
            if (!statementContext.arePointQueryFixedKeyConstraintsComplete()) {
                return unsupported();
            }

            Map<String, Integer> keyOrdinals = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            List<List<Literal>> fixedConstraints = new ArrayList<>(keyColumns.size());
            for (int ordinal = 0; ordinal < keyColumns.size(); ordinal++) {
                keyOrdinals.put(keyColumns.get(ordinal).getName(), ordinal);
                fixedConstraints.add(new ArrayList<>());
            }

            List<PlaceholderKeyBinding> placeholderBindings = new ArrayList<>();
            for (Map.Entry<PlaceholderId, SlotReference> entry
                    : statementContext.getIdToComparisonSlot().entrySet()) {
                SlotReference slot = entry.getValue();
                if (!slot.getOriginalColumn().isPresent()) {
                    return unsupported();
                }
                Integer ordinal = keyOrdinals.get(slot.getOriginalColumn().get().getName());
                if (ordinal == null) {
                    return unsupported();
                }
                placeholderBindings.add(new PlaceholderKeyBinding(entry.getKey(), ordinal, slot));
            }

            List<Placeholder> placeholders = statementContext.getPlaceholders();
            if (placeholderBindings.size() != placeholders.size()) {
                return unsupported();
            }
            for (Placeholder placeholder : placeholders) {
                if (!statementContext.getIdToComparisonSlot().containsKey(placeholder.getPlaceholderId())) {
                    return unsupported();
                }
            }

            for (PointQueryFixedKeyConstraint constraint
                    : statementContext.getPointQueryFixedKeyConstraints()) {
                SlotReference slot = constraint.getSlot();
                if (!slot.getOriginalColumn().isPresent()) {
                    return unsupported();
                }
                Integer ordinal = keyOrdinals.get(slot.getOriginalColumn().get().getName());
                if (ordinal != null) {
                    fixedConstraints.get(ordinal).add(constraint.getLiteral());
                } else if (!Column.DELETE_SIGN.equals(slot.getOriginalColumn().get().getName())) {
                    return unsupported();
                }
            }

            boolean[] covered = new boolean[keyColumns.size()];
            for (PlaceholderKeyBinding binding : placeholderBindings) {
                covered[binding.keyOrdinal] = true;
            }
            for (int ordinal = 0; ordinal < fixedConstraints.size(); ordinal++) {
                covered[ordinal] |= !fixedConstraints.get(ordinal).isEmpty();
            }
            for (boolean keyCovered : covered) {
                if (!keyCovered) {
                    return unsupported();
                }
            }
            return new PointQueryKeyTemplate(keyColumns, placeholderBindings, fixedConstraints, true);
        }

        private PointQueryExecutionContext bind(StatementContext statementContext) {
            if (!complete || statementContext == null) {
                return PointQueryExecutionContext.fallback();
            }
            List<List<Literal>> valuesByKey = new ArrayList<>(fixedConstraints.size());
            for (List<Literal> constraints : fixedConstraints) {
                valuesByKey.add(new ArrayList<>(constraints));
            }
            for (PlaceholderKeyBinding binding : placeholderBindings) {
                Expression value = statementContext.getIdToPlaceholderRealExpr().get(binding.placeholderId);
                if (!(value instanceof Literal)) {
                    return PointQueryExecutionContext.fallback();
                }
                Literal typedValue = coerceComparisonLiteral(binding.slot, (Literal) value);
                if (typedValue == null) {
                    return PointQueryExecutionContext.fallback();
                }
                if (typedValue instanceof NullLiteral) {
                    return PointQueryExecutionContext.empty();
                }
                valuesByKey.get(binding.keyOrdinal).add(typedValue);
            }

            Map<String, LiteralExpr> keyValues = new LinkedHashMap<>();
            for (int ordinal = 0; ordinal < keyColumns.size(); ordinal++) {
                List<Literal> values = valuesByKey.get(ordinal);
                if (values.isEmpty()) {
                    return PointQueryExecutionContext.fallback();
                }
                Literal representative = values.get(0);
                if (representative instanceof NullLiteral) {
                    return PointQueryExecutionContext.empty();
                }
                for (int i = 1; i < values.size(); i++) {
                    Boolean equal = sqlEquals(representative, values.get(i));
                    if (equal == null) {
                        return PointQueryExecutionContext.fallback();
                    }
                    if (!equal) {
                        return PointQueryExecutionContext.empty();
                    }
                }
                LiteralExpr physicalValue = toPhysicalKeyLiteral(representative, keyColumns.get(ordinal));
                if (physicalValue == null) {
                    return PointQueryExecutionContext.fallback();
                }
                keyValues.put(keyColumns.get(ordinal).getName(), physicalValue);
            }
            return PointQueryExecutionContext.lookup(keyValues);
        }

        private static Literal coerceComparisonLiteral(SlotReference slot, Literal value) {
            try {
                Expression comparison = TypeCoercionUtils.processComparisonPredicate(new EqualTo(slot, value));
                Expression comparisonSlot = comparison.child(0);
                // A cast on the physical key can change equality semantics (for example INT 1
                // compared with string '01'). Normal planning must evaluate such comparisons.
                return comparisonSlot instanceof SlotReference && comparison.child(1) instanceof Literal
                        ? (Literal) comparison.child(1) : null;
            } catch (Exception e) {
                return null;
            }
        }

        private static Boolean sqlEquals(Literal left, Literal right) {
            if (left instanceof NullLiteral || right instanceof NullLiteral) {
                return false;
            }
            try {
                Expression comparison = TypeCoercionUtils.processComparisonPredicate(new EqualTo(left, right));
                Expression result = FoldConstantRuleOnFE.evaluateWithoutContext(comparison);
                return result instanceof BooleanLiteral ? ((BooleanLiteral) result).getValue() : null;
            } catch (Exception e) {
                return null;
            }
        }

        private static LiteralExpr toPhysicalKeyLiteral(Literal literal, Column column) {
            try {
                LiteralExpr legacyLiteral = literal.toLegacyLiteral();
                Type columnType = column.getType();
                if (!columnType.equals(legacyLiteral.getType())
                        && !columnType.matchesType(legacyLiteral.getType())) {
                    legacyLiteral = LiteralExprUtils.createLiteral(legacyLiteral.getStringValue(), columnType);
                }
                return legacyLiteral;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private static class PlaceholderKeyBinding {
        private final PlaceholderId placeholderId;
        private final int keyOrdinal;
        private final SlotReference slot;

        private PlaceholderKeyBinding(PlaceholderId placeholderId, int keyOrdinal, SlotReference slot) {
            this.placeholderId = placeholderId;
            this.keyOrdinal = keyOrdinal;
            this.slot = slot;
        }
    }

    /** Immutable outcome and typed key tuple for exactly one point-query execution. */
    public static class PointQueryExecutionContext {
        public enum Decision {
            LOOKUP,
            EMPTY,
            FALLBACK
        }

        private final Decision decision;
        private final Map<String, LiteralExpr> keyValues;

        private PointQueryExecutionContext(Decision decision, Map<String, LiteralExpr> keyValues) {
            this.decision = decision;
            this.keyValues = Collections.unmodifiableMap(new LinkedHashMap<>(keyValues));
        }

        public static PointQueryExecutionContext lookup(Map<String, LiteralExpr> keyValues) {
            return new PointQueryExecutionContext(Decision.LOOKUP, keyValues);
        }

        public static PointQueryExecutionContext empty() {
            return new PointQueryExecutionContext(Decision.EMPTY, Collections.emptyMap());
        }

        public static PointQueryExecutionContext fallback() {
            return new PointQueryExecutionContext(Decision.FALLBACK, Collections.emptyMap());
        }

        public Decision getDecision() {
            return decision;
        }

        public Map<String, LiteralExpr> getKeyValues() {
            return keyValues;
        }
    }
}

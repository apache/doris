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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorVectorSearch;
import org.apache.doris.datasource.connector.converter.VectorSearchPredicateConverter;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.scan.ConnectorVectorSearchPlanner;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TVectorElementType;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Preserve the global TopN and let a connector admit bounded search on each of its ranges. */
public class PushDownVectorTopNIntoFileScan implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFileScan())).then(top ->
                        pushDown(top, top.child(), top.child().child(), Optional.empty()))
                        .toRule(RuleType.PUSH_DOWN_VECTOR_TOPN_INTO_FILE_SCAN),
                logicalTopN(logicalProject(logicalFilter(logicalFileScan()))).then(top ->
                        pushDown(top, top.child(), top.child().child().child(), Optional.of(top.child().child())))
                        .toRule(RuleType.PUSH_DOWN_VECTOR_TOPN_INTO_FILE_SCAN));
    }

    private Plan pushDown(LogicalTopN<?> top, LogicalProject<?> project,
            LogicalFileScan scan, Optional<LogicalFilter<?>> filter) {
        if (!(scan.getTable() instanceof PluginDrivenExternalTable) || scan.getVectorSearch().isPresent()
                || scan.getTableSample().isPresent() || top.getOrderKeys().size() != 1 || top.getLimit() <= 0
                || top.getOffset() > Integer.MAX_VALUE - top.getLimit()
                || !ConnectContext.get().getSessionVariable().enableFileScannerV2
                || scan.getScanParams().map(params -> params.incrementalRead()).orElse(false)) {
            return null;
        }
        Alias key = project.getProjects().stream().filter(expr -> expr instanceof Alias
                && expr.toSlot().equals(top.getOrderKeys().get(0).getExpr()))
                .map(Alias.class::cast).findFirst().orElse(null);
        if (key == null) {
            return null;
        }
        Expression distance = key.child();
        boolean l2 = distance instanceof L2DistanceApproximate;
        if (!(l2 && top.getOrderKeys().get(0).isAsc())
                && !(distance instanceof InnerProductApproximate && !top.getOrderKeys().get(0).isAsc())) {
            return null;
        }
        if (!(distance.child(0) instanceof SlotReference) || !(distance.child(1) instanceof ArrayLiteral)) {
            return null;
        }
        SlotReference vectorColumn = (SlotReference) distance.child(0);
        List<Literal> query = ((ArrayLiteral) distance.child(1)).getValue();
        if (query.isEmpty() || query.stream().anyMatch(value -> value instanceof NullLiteral
                || !Float.isFinite((float) value.getDouble()))) {
            return null;
        }
        // Moving a bounded read below assert_true/random-like projections may suppress evaluation/errors.
        for (NamedExpression output : project.getProjects()) {
            if (output.anyMatch(expr -> expr instanceof NoneMovableFunction
                    || ((Expression) expr).getDataType().isVariantType()
                    || ((Expression) expr).getDataType().isTimeStampNsType())) {
                return null;
            }
        }
        if (!complexColumnsRideAsWholeColumns(project, key, distance)) {
            return null;
        }
        List<ConnectorExpression> predicates = new ArrayList<>();
        if (filter.isPresent()) {
            for (Expression conjunct : filter.get().getConjuncts()) {
                ConnectorExpression converted = VectorSearchPredicateConverter.convert(conjunct);
                if (converted == null) {
                    return null;
                }
                predicates.add(converted);
            }
        }
        Optional<ConnectorExpression> predicate = predicates.isEmpty() ? Optional.empty()
                : Optional.of(predicates.size() == 1 ? predicates.get(0) : new ConnectorAnd(predicates));
        ByteBuffer values = ByteBuffer.allocate(query.size() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        query.forEach(value -> values.putFloat((float) value.getDouble()));
        TVectorSearchParams vector = new TVectorSearchParams().setColumn(
                vectorColumn.getOriginalColumn().map(Column::getName).orElse(vectorColumn.getName()))
                .setQueryVector(new TSearchVector().setElementType(TVectorElementType.FLOAT32)
                        .setDimension(query.size()).setValues(values.array()))
                .setMetric(l2 ? TVectorMetric.L2 : TVectorMetric.DOT_PRODUCT)
                .setTopK(top.getLimit() + top.getOffset()).setOffset(0);
        ConnectorVectorSearch requested = new ConnectorVectorSearch(new TExternalSearchRequest()
                .setSchemaVersion(1).setSearchQuery(TExternalSearchQuery.vector_search(vector)),
                "", false, predicate);
        Optional<ConnectorVectorSearch> admitted = ConnectorVectorSearchPlanner.prepare(
                (PluginDrivenExternalTable) scan.getTable(), scan.getTableSnapshot(), scan.getScanParams(), requested);
        if (!admitted.isPresent()) {
            return null;
        }
        Column column = new Column(admitted.get().getDistanceColumn(), Type.FLOAT, false,
                "Reader-produced vector distance");
        column.setIsVisible(false);
        SlotReference score = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(),
                scan.getTable(), column, scan.getQualifier());
        Map<Expression, Expression> replacements = Collections.singletonMap(distance, score);
        Plan child = scan.withVectorSearch(score, admitted.get());
        if (filter.isPresent()) {
            child = filter.get().withConjunctsAndChild(filter.get().getConjuncts(), child);
        }
        return top.withChildren(project.withProjectsAndChild(
                ExpressionUtils.replaceNamedExpressions(project.getProjects(), replacements), child));
    }

    private static boolean complexColumnsRideAsWholeColumns(LogicalProject<?> project,
            Alias orderKeyAlias, Expression orderKeyExpr) {
        for (NamedExpression projection : project.getProjects()) {
            if (!(projection instanceof Alias)) {
                // A bare passthrough slot output: whole unless already pruned.
                if (projection instanceof SlotReference
                        && !slotRidesWholeColumn((SlotReference) projection)) {
                    return false;
                }
                continue;
            }
            Alias alias = (Alias) projection;
            if (alias.equals(orderKeyAlias)) {
                continue;
            }
            if (alias.child() instanceof SlotReference) {
                if (!slotRidesWholeColumn((SlotReference) alias.child())) {
                    return false;
                }
                continue;
            }
            Set<SlotReference> replacedSlots = new HashSet<>();
            Set<Expression> distanceCalls = alias.child().collect(e ->
                    e instanceof L2DistanceApproximate || e instanceof InnerProductApproximate);
            // Only the call the rewrite actually replaces (the one equal to the
            // order-key expression) may exempt its slots: a distinct distance call
            // over a complex column stays in the projection after the rewrite, so
            // its complex slots must keep the whole-column guard.
            for (Expression call : distanceCalls) {
                if (call.equals(orderKeyExpr)) {
                    replacedSlots.addAll(call.collect(SlotReference.class::isInstance));
                }
            }
            Set<SlotReference> slots = alias.child().collect(SlotReference.class::isInstance);
            for (SlotReference slot : slots) {
                if (!replacedSlots.contains(slot)) {
                    if (slot.getDataType().isComplexType() || !slotRidesWholeColumn(slot)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * A whole-riding slot keeps the type it was created with from the original
     * column ({@code SlotReference.fromColumn}: {@code DataType.fromCatalogType}).
     * Nested pruning materializes a REDUCED type ({@code ROW<b>} from
     * {@code ROW<a,b>}) while the original column keeps the full one; only a whole
     * slot may ride the vector path, matching the desc-side backstop's
     * slot-versus-column comparison.
     */
    private static boolean slotRidesWholeColumn(SlotReference slot) {
        if (!slot.getDataType().isComplexType() || !slot.getOriginalColumn().isPresent()) {
            return true;
        }
        return slot.getDataType().equals(org.apache.doris.nereids.types.DataType.fromCatalogType(
                slot.getOriginalColumn().get().getType()));
    }

}

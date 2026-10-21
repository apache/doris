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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.source.LancePredicateConverter;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sqrt;
import org.apache.doris.nereids.trees.expressions.functions.table.LanceSearch;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.tablefunction.FullTextSearchTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;
import org.apache.doris.tablefunction.VectorSearchTableValuedFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Reuses the fixed-snapshot Lance search relations for ordinary SQL search expressions. */
public class PushDownSearchTopNIntoLanceScan extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalTopN(logicalProject(any()))
                .thenApply(ctx -> rewrite(ctx.root, ctx.cascadesContext))
                .toRule(RuleType.PUSH_DOWN_SEARCH_TOPN_INTO_LANCE_SCAN);
    }

    private Plan rewrite(LogicalTopN<LogicalProject<Plan>> topN, CascadesContext context) {
        LogicalProject<Plan> project = topN.child();
        Plan input = project.child();
        Set<Expression> predicates = ImmutableSet.of();
        if (input instanceof LogicalFilter) {
            predicates = ((LogicalFilter<?>) input).getConjuncts();
            input = input.child(0);
        }
        // A non-movable projection may raise an error on rows outside native Top-K.
        // Keep its evaluation before pruning, as LogicalProject.pruneOutputs requires.
        if (!(input instanceof LogicalFileScan) || !(((LogicalFileScan) input).getTable()
                instanceof LanceExternalTable) || project.isDistinct()
                || project.containsNoneMovableFunction()) {
            return null;
        }
        LogicalFileScan scan = (LogicalFileScan) input;
        if (scan.getTableSample().isPresent() || scan.getScanParams().isPresent()
                || topN.getOrderKeys().size() != 1 || topN.getLimit() <= 0) {
            return null;
        }
        Expression order = topN.getOrderKeys().get(0).getExpr();
        Expression expression = project.getProjects().stream()
                .filter(p -> p.toSlot().equals(order) && p instanceof Alias)
                .map(p -> ((Alias) p).child()).findFirst().orElse(null);
        boolean l2 = expression instanceof L2DistanceApproximate;
        boolean dot = expression instanceof InnerProductApproximate;
        boolean score = expression instanceof Score;
        if (!l2 && !dot && !score) {
            return null;
        }
        // The explicit approximate functions select native Lance candidate semantics: NULL
        // vectors and NaN distances are excluded. Exact functions retain scalar validation.
        // Each split can prune only the nearest (or highest-scoring) candidates. Reversing
        // that order or adding tie-breakers would require a different local candidate contract.
        if (topN.getOrderKeys().get(0).isAsc() != l2) {
            if (score) {
                throw new AnalysisException("Lance BM25 Top-N requires ORDER BY score() DESC");
            }
            return null;
        }
        if (topN.getOffset() < 0 || topN.getLimit() > 0xffff_ffffL - topN.getOffset()) {
            // UINT32 bounds the native candidate budget, not ordinary scalar SQL LIMIT/OFFSET.
            if (!score) {
                return null;
            }
            throw new AnalysisException("Lance search requires limit + offset <= 4294967295");
        }
        LanceExternalTable table = (LanceExternalTable) scan.getTable();
        LanceTableMetadata metadata = table.getMetadata(scan.getRelationSnapshot());
        if (!score && metadata.getSchema().getFields().stream().anyMatch(
                field -> field.getName().equalsIgnoreCase(VectorSearchTableValuedFunction.DISTANCE_COLUMN))) {
            // The TVF reserves _distance, but an ordinary table may legally store that column.
            // Keep its existing scalar execution rather than turning a valid SQL query into an error.
            return null;
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table", scan.qualified().stream()
                .map(part -> "`" + part.replace("`", "``") + "`").collect(Collectors.joining(".")));
        // OFFSET belongs to the global Doris TopN, never independently to each split.
        properties.put("top_k", Long.toString(topN.getLimit() + topN.getOffset()));
        properties.put("version", Long.toString(metadata.getVersion()));
        List<Expression> filters = new ArrayList<>(predicates);
        List<Expression> scoreFilters = new ArrayList<>();
        if (score) {
            List<Match> matches = predicates.stream().filter(Match.class::isInstance)
                    .map(Match.class::cast).collect(Collectors.toList());
            if (matches.size() != 1) {
                throw new AnalysisException("Lance BM25 Top-N requires one top-level MATCH_ANY, MATCH_ALL "
                        + "or MATCH_PHRASE predicate on a single column");
            }
            Match match = matches.get(0);
            if (!(match.left() instanceof SlotReference) || !(match.right() instanceof StringLikeLiteral)
                    || match.getAnalyzer().isPresent()) {
                throw new AnalysisException("Lance MATCH requires a string literal and the index's stored analyzer");
            }
            properties.put("column", physicalName((SlotReference) match.left()));
            properties.put("query", ((StringLikeLiteral) match.right()).getStringValue());
            switch (match.op()) {
                case MATCH_ANY:
                    properties.put("operator", "or");
                    break;
                case MATCH_ALL:
                    properties.put("operator", "and");
                    break;
                case MATCH_PHRASE:
                    properties.put("query_type", "phrase");
                    break;
                default:
                    throw new AnalysisException("Unsupported Lance BM25 MATCH operator: " + match.op());
            }
            filters.remove(match);
            for (Expression predicate : filters) {
                if (predicate.anyMatch(Score.class::isInstance)) {
                    if (!isScoreLowerBound(predicate)) {
                        throw new AnalysisException("Lance BM25 supports only score() > literal or score() >= literal");
                    }
                    scoreFilters.add(predicate);
                }
            }
            filters.removeAll(scoreFilters);
        } else {
            Expression column = expression.child(0);
            if (column instanceof Cast && column.getDataType().equals(column.child(0).getDataType())) {
                column = column.child(0);
            }
            Expression query = expression.child(1);
            if (!(column instanceof SlotReference) || !(query instanceof ArrayLiteral)) {
                return null;
            }
            Field field = metadata.getSchema().findField(physicalName((SlotReference) column));
            // These SQL functions consume ARRAY<FLOAT>. Searching a wider physical vector
            // directly would bypass Doris's element casts and can change distances/ranking.
            if (field == null || !(field.getType() instanceof ArrowType.FixedSizeList)
                    || field.getChildren().size() != 1
                    || !(field.getChildren().get(0).getType() instanceof ArrowType.FloatingPoint)
                    || ((ArrowType.FloatingPoint) field.getChildren().get(0).getType()).getPrecision()
                            != FloatingPointPrecision.SINGLE) {
                return null;
            }
            properties.put("column", field.getName());
            properties.put("query_vector", query.toSql());
            properties.put("metric", l2 ? "l2" : "dot");
        }
        Optional<byte[]> filter = convertFilters(filters, scan, metadata, context);
        if (!filter.isPresent()) {
            if (score) {
                throw new AnalysisException("Lance BM25 WHERE conditions must be fully pushable before Top-N");
            }
            return null;
        }
        metadata = withIndexes(table, metadata);
        // SDK schema conversion can leave ordinary fragment scans usable without Lance field IDs.
        // Missing index-planning metadata must not turn that scalar path into an analysis error.
        if (!score && !metadata.getLanceFieldId(properties.get("column")).isPresent()) {
            return null;
        }
        TableValuedFunctionIf function;
        String name;
        String resultName;
        try {
            if (score) {
                function = FullTextSearchTableValuedFunction.forScan(properties, table, metadata, filter.get());
                name = FullTextSearchTableValuedFunction.NAME;
                resultName = FullTextSearchTableValuedFunction.SCORE_COLUMN;
            } else {
                function = VectorSearchTableValuedFunction.forScan(properties, table, metadata, filter.get());
                name = VectorSearchTableValuedFunction.NAME;
                resultName = VectorSearchTableValuedFunction.DISTANCE_COLUMN;
            }
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        LogicalTVFRelation relation = new LogicalTVFRelation(scan.getRelationId(),
                new LanceSearch(name, new Properties(properties), function), ImmutableList.of());
        Map<String, SlotReference> sourceSlots = new HashMap<>();
        scan.getOutput().forEach(slot -> sourceSlots.put(physicalName((SlotReference) slot), (SlotReference) slot));
        List<Slot> outputs = new ArrayList<>();
        Slot result = null;
        for (Slot slot : relation.getOutput()) {
            if (slot.getName().equals(resultName)) {
                result = slot;
            }
            SlotReference source = sourceSlots.get(slot.getName());
            outputs.add(source == null ? slot : ((SlotReference) slot)
                    .withExprId(source.getExprId()).withQualifier(source.getQualifier()));
        }
        if (result == null) {
            throw new AnalysisException("Lance search result column is missing: " + resultName);
        }
        Expression value = score ? result : l2
                ? new Cast(new Sqrt(new Cast(result, DoubleType.INSTANCE)), FloatType.INSTANCE)
                : new Subtract(new FloatLiteral(1.0f), result);
        Map<Expression, Expression> replacements = new HashMap<>();
        replacements.put(expression, value);
        List<NamedExpression> projects = ExpressionUtils.replaceNamedExpressions(project.getProjects(), replacements);
        List<Expression> residuals = scoreFilters.stream().map(e -> ExpressionUtils.replace(e, replacements))
                .collect(Collectors.toList());
        Set<Slot> required = new HashSet<>();
        projects.forEach(p -> required.addAll(p.getInputSlots()));
        residuals.forEach(p -> required.addAll(p.getInputSlots()));
        // This rewrite runs after column pruning. Do not reintroduce the embedding or wide
        // payload columns just because they are part of the underlying search TVF's schema.
        relation = relation.withCachedOutputs(outputs.stream().filter(required::contains).collect(Collectors.toList()));
        Plan child = residuals.isEmpty() ? relation : new LogicalFilter<>(ImmutableSet.copyOf(residuals), relation);
        context.getStatementContext().getSqlCacheContext().ifPresent(cache -> cache.setCannotProcessExpression(true));
        return topN.withChildren(project.withProjectsAndChild(projects, child));
    }

    private Optional<byte[]> convertFilters(List<Expression> filters, LogicalFileScan scan,
            LanceTableMetadata metadata, CascadesContext context) {
        if (filters.isEmpty()) {
            return Optional.of(new byte[0]);
        }
        PlanTranslatorContext translator = new PlanTranslatorContext(context);
        TupleDescriptor tuple = translator.getDescTable().createTupleDescriptor();
        scan.getOutput().forEach(slot -> translator.createSlotDesc(tuple, (SlotReference) slot, scan.getTable()));
        List<Expr> expressions = filters.stream().map(e -> ExpressionTranslator.translate(e, translator))
                .collect(Collectors.toList());
        LancePredicateConverter.ConversionResult result = new LancePredicateConverter(metadata.getSchema())
                .convert(expressions);
        return result.getResidualConjuncts().isEmpty() ? Optional.of(result.getSubstraitFilter()) : Optional.empty();
    }

    private LanceTableMetadata withIndexes(LanceExternalTable table, LanceTableMetadata metadata) {
        if (metadata.getIndexMetadataState().canPlanIndexSegments()) {
            return metadata;
        }
        Optional<TableSnapshot> version = Optional.of(new TableSnapshot(Long.toString(metadata.getVersion()),
                TableSnapshot.VersionType.VERSION));
        LanceRefSelector selector = metadata.getBranch().isPresent()
                ? LanceRefSelector.branch(metadata.getBranch().get(), version) : LanceRefSelector.snapshot(version);
        LanceTableMetadata loaded = table.loadMetadataForSearch(selector);
        if (loaded.getVersion() != metadata.getVersion() || !loaded.getDatasetUri().equals(metadata.getDatasetUri())
                || !loaded.getSchema().equals(metadata.getSchema())) {
            throw new AnalysisException("Lance search index metadata does not match the bound snapshot");
        }
        return loaded;
    }

    private static String physicalName(SlotReference slot) {
        return slot.getOriginalColumn().map(org.apache.doris.catalog.Column::getName).orElse(slot.getName());
    }

    private boolean isScoreLowerBound(Expression predicate) {
        if ((predicate instanceof GreaterThan || predicate instanceof GreaterThanEqual)
                && unwrapScore(predicate.child(0)) instanceof Score && predicate.child(1) instanceof Literal) {
            return true;
        }
        return (predicate instanceof LessThan || predicate instanceof LessThanEqual)
                && predicate.child(0) instanceof Literal && unwrapScore(predicate.child(1)) instanceof Score;
    }

    private Expression unwrapScore(Expression expression) {
        while (expression instanceof Cast && (expression.getDataType().isFloatType()
                || expression.getDataType().isDoubleType())) {
            expression = expression.child(0);
        }
        return expression;
    }
}

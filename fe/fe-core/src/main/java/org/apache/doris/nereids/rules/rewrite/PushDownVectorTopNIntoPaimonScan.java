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

import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.source.PaimonVectorSearch;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.AnnTopNInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TVectorMetric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Push a primary-key vector (ANN) top-N down into the Paimon scan (index-only).
 *
 * <p>Recognizes {@code TopN[order by dis] -> Project[dis := dist_fn(vec, q)] -> [Filter] -> PaimonScan}
 * where {@code dist_fn} is {@code l2_distance_approximate} (ASC) or
 * {@code inner_product_approximate} (DESC) and the Paimon table has a PK-vector
 * index on {@code vec} with a matching distance metric.
 *
 * <p>The rewrite mirrors the internal-table ANN path (see
 * {@code PushDownVectorTopNIntoOlapScan}): the scan is told to produce a
 * reader-generated {@code __paimon_search_score_to_dis} column via the PK-vector index, and
 * every occurrence of {@code dist_fn(vec, q)} is replaced by that column's slot. The
 * order key slot keeps its identity, so the surrounding TopN merges/limits unchanged.
 * The distance always comes from the index, never from the raw vector.
 *
 * <p>No arithmetic is emitted here even though paimon-rust reports a score rather than
 * a distance: the BE reader undoes the metric-dependent score transform in place while
 * filling the block, exactly as the internal table sqrt's faiss's squared distance inside
 * the index layer. So the column arrives already holding what {@code dist_fn} would
 * return, and the plan stays a bare slot reference -- which also keeps {@code dis}
 * non-nullable (an expression over the score would not be, since {@code Divide} and
 * {@code Sqrt} are always nullable) and makes predicates like
 * {@code dist_fn(vec, q) < 10} correct without any direction flip.
 *
 * <p>Being index-only is just an optimization here, not a precondition: if nothing
 * else references the raw vector column it is left unreferenced and dropped by column
 * pruning (pure index-only read); if the query still needs it ({@code SELECT vec} or a
 * predicate on {@code vec}), it stays a normal projected column and the BE vector
 * reader materializes it alongside the score. Either way ANN is applied -- the rewrite
 * never falls back merely because the vector is read.
 */
public class PushDownVectorTopNIntoPaimonScan implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFileScan())).when(t -> t.getOrderKeys().size() == 1).then(topN -> {
                    LogicalProject<LogicalFileScan> project = topN.child();
                    LogicalFileScan scan = project.child();
                    return pushDown(topN, project, scan, Optional.empty());
                }).toRule(RuleType.PUSH_DOWN_VECTOR_TOPN_INTO_PAIMON_SCAN),
                logicalTopN(logicalProject(logicalFilter(logicalFileScan())))
                        .when(t -> t.getOrderKeys().size() == 1).then(topN -> {
                            LogicalProject<LogicalFilter<LogicalFileScan>> project = topN.child();
                            LogicalFilter<LogicalFileScan> filter = project.child();
                            LogicalFileScan scan = filter.child();
                            return pushDown(topN, project, scan, Optional.of(filter));
                        }).toRule(RuleType.PUSH_DOWN_VECTOR_TOPN_INTO_PAIMON_SCAN)
        );
    }

    private Plan pushDown(
            LogicalTopN<?> topN,
            LogicalProject<?> project,
            LogicalFileScan scan,
            Optional<LogicalFilter<?>> optionalFilter) {
        // Only Paimon external tables carry PK-vector indexes.
        if (!(scan.getTable() instanceof PaimonExternalTable)) {
            return null;
        }

        // The index-only search path is executed exclusively by the BE paimon-rust reader,
        // which only FileScannerV2 can select (the V1 FileScanner explicitly rejects
        // PAIMON_RUST). If the rust reader or the V2 scanner is disabled, the split would
        // fall through to the JNI reader, which ignores the vector payload and would
        // return a non-ANN scan with an empty distance column. Fall back to a normal
        // scan then.
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getSessionVariable() == null
                || !ctx.getSessionVariable().isEnablePaimonRustReader()
                || !ctx.getSessionVariable().enableFileScannerV2) {
            return null;
        }

        // The order key must be a SlotReference produced by a Project alias.
        Expression orderKey = topN.getOrderKeys().get(0).getExpr();
        if (!(orderKey instanceof SlotReference)) {
            return null;
        }
        SlotReference keySlot = (SlotReference) orderKey;
        Alias orderKeyAlias = null;
        Expression orderKeyExpr = null;
        for (NamedExpression projection : project.getProjects()) {
            if (projection.toSlot().equals(keySlot) && projection instanceof Alias) {
                orderKeyAlias = (Alias) projection;
                orderKeyExpr = orderKeyAlias.child();
                break;
            }
        }
        if (orderKeyExpr == null) {
            return null;
        }

        // l2 must sort ascending (smaller distance = closer); inner_product descending.
        boolean isAsc = topN.getOrderKeys().get(0).isAsc();
        boolean l2Dist = orderKeyExpr instanceof L2DistanceApproximate;
        boolean innerProduct = orderKeyExpr instanceof InnerProductApproximate;
        if (!(l2Dist && isAsc) && !(innerProduct && !isAsc)) {
            return null;
        }

        Expression left;
        Expression right;
        if (l2Dist) {
            left = ((L2DistanceApproximate) orderKeyExpr).left();
            right = ((L2DistanceApproximate) orderKeyExpr).right();
        } else {
            left = ((InnerProductApproximate) orderKeyExpr).left();
            right = ((InnerProductApproximate) orderKeyExpr).right();
        }
        while (left instanceof Cast) {
            left = ((Cast) left).child();
        }
        if (!(left instanceof SlotReference && right.isConstant() && right instanceof ArrayLiteral)) {
            return null;
        }
        SlotReference vectorSlot = (SlotReference) left;
        if (!vectorSlot.getOriginalColumn().isPresent() || !vectorSlot.getOriginalTable().isPresent()) {
            return null;
        }
        String annColumnName = vectorSlot.getOriginalColumn().get().getName();

        // Validate the PK-vector index and metric on the Paimon table.
        PaimonExternalTable paimonTable = (PaimonExternalTable) scan.getTable();
        CoreOptions coreOptions;
        try {
            Table basePaimonTable = paimonTable.getPaimonTable(Optional.empty());
            coreOptions = CoreOptions.fromMap(basePaimonTable.options());
        } catch (Throwable t) {
            return null;
        }
        if (!coreOptions.primaryKeyVectorIndexEnabled()) {
            return null;
        }
        boolean columnIndexed = coreOptions.primaryKeyVectorIndexColumns().stream()
                .anyMatch(c -> c.equalsIgnoreCase(annColumnName));
        if (!columnIndexed) {
            return null;
        }
        // v1 supports only l2 / inner_product; the function must match the index metric.
        String metric = coreOptions.primaryKeyVectorDistanceMetric(annColumnName);
        String expectedMetric = l2Dist ? "l2" : "inner_product";
        if (!expectedMetric.equals(metric)) {
            return null;
        }
        // The thrift-level enum the AnnTopNInfo will carry. expectedMetric is kept as a
        // string for the comparison above (coreOptions returns a string), but the
        // AnnTopNInfo / wire contract uses TVectorMetric, so map once here. DOT_PRODUCT
        // is the thrift enum that paimon-rust's "inner_product" score transform maps to
        // (see the BE reader's parse_score_transform).
        TVectorMetric expectedMetricEnum = l2Dist ? TVectorMetric.L2 : TVectorMetric.DOT_PRODUCT;
        // NOTE: the FE-side data-predicate interception is intentionally not done. The
        // data predicate is handed in full to the BE via the vector search builder's
        // residual filter, where the Rust candidate search stage applies it together
        // with deletion vectors. If some table shape cannot be handled correctly, it is
        // Rust's responsibility to reject it or return correct data, not the planner's.

        // paimon-rust's vector index operates on f32, so carry the query as floats
        // rather than List<Double>: it avoids a double→float narrowing on the wire
        // and lets PaimonScanNode encode straight into a FLOAT32 TSearchVector.
        List<Literal> vectorLiterals = ((ArrayLiteral) right).getValue();
        float[] annQueryVector = new float[vectorLiterals.size()];
        for (int i = 0; i < vectorLiterals.size(); i++) {
            annQueryVector[i] = (float) vectorLiterals.get(i).getDouble();
        }

        // Build the reader-produced distance column, matched by name downstream (BE fills
        // any block column named __paimon_search_score_to_dis, bridging it from
        // paimon-rust's __paimon_search_score Arrow field). It holds the metric-native
        // distance, not the score: the BE reader converts it in place while filling the
        // block, so nothing has to be computed here. It is backed by a synthetic hidden
        // Column so the SlotDescriptor keeps a non-null column through
        // FileQueryScanNode.initSchemaParams; the column is excluded from the
        // Parquet/ORC column-position mapping on the scan node.
        SlotReference distanceSlot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), scan.getTable(),
                PaimonVectorSearch.getDistanceColumn(), scan.qualified());

        // Reuse the index-produced value everywhere the same distance expression
        // appears, mirroring PushDownVectorTopNIntoOlapScan:
        //   - orderKeyAlias  -> matches the `dis` projection at its Alias root, so that
        //     item is rebuilt as `dis := <distance slot>` in one step.
        //   - orderKeyExpr   -> matches the bare `dist_fn(vec, q)` wherever else it
        //     occurs (filter conjuncts, other projections), so those reuse the index
        //     value instead of recomputing from the raw vector.
        // Both keys map to the same slot, so `dis` stays numerically identical to Doris
        // semantics on every path. Note ExpressionUtils.replace rewrites top-down and
        // short-circuits on a hit, and Alias.withChildren preserves the ExprId/name, so
        // the alias identity the TopN references is never disturbed.
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        replaceMap.put(orderKeyAlias, distanceSlot);
        replaceMap.put(orderKeyExpr, distanceSlot);

        Plan plan = scan.withVectorTopN(
                distanceSlot,
                new AnnTopNInfo(annQueryVector, annColumnName,
                        topN.getLimit() + topN.getOffset(), expectedMetricEnum));
        if (optionalFilter.isPresent()) {
            LogicalFilter<?> filter = optionalFilter.get();
            Set<Expression> newConjuncts = ExpressionUtils.replace(filter.getConjuncts(), replaceMap);
            plan = filter.withConjunctsAndChild(newConjuncts, plan);
        }

        List<NamedExpression> newProjections = ExpressionUtils
                .replaceNamedExpressions(project.getProjects(), replaceMap);
        LogicalProject<?> newProject = project.withProjectsAndChild(newProjections, plan);
        return topN.withChildren(newProject);
    }
}

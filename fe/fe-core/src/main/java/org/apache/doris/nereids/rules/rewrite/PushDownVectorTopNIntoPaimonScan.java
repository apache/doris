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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.source.PaimonVectorSearch;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

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

    /** Doris column types the BE paimon-rust predicate converter can push; see
     *  PaimonRustPredicateConverter._is_supported_slot_type on the BE side. */
    private static final Set<Type> VECTOR_FILTER_SUPPORTED_COLUMN_TYPES = ImmutableSet.of(
            Type.BOOLEAN, Type.TINYINT, Type.SMALLINT, Type.INT, Type.BIGINT,
            Type.STRING, Type.DATE, Type.DATEV2);

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

        // An incremental scan (t@incr / BETWEEN) must not be rewritten into a
        // PK-vector search: the VectorScan builder has no incremental mode, so a
        // vector payload would silently return snapshot rows for a query that asked
        // for changes. Keeping the query un-rewritten runs it as the ordinary
        // incremental scan the user requested, with the distance computed in Doris.
        // (The scan node's vector gate keeps !isIncremental as the authoritative
        // backstop for any path that produces a vector split regardless.)
        //
        // The same holds for a pinned file-creation-time read state: the ordinary
        // path enforces `scan.file-creation-time-millis` by dropping manifest
        // entries older than the bound, but the vector search plans its own splits
        // and never consults that bound, so old files' nearest neighbors could leak
        // into the result. Snapshot/tag/version selectors are fine -- they are
        // resolved into the table handle before the vector search -- so only the
        // file-level bound forces the decline.
        if (scan.getScanParams().isPresent()) {
            TableScanParams scanParams = scan.getScanParams().get();
            if (scanParams.incrementalRead()) {
                return null;
            }
            Map<String, String> rawParams = scanParams.getMapParams();
            if (rawParams.containsKey(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())) {
                return null;
            }
        }

        // A bounded vector top-N is only correct when every filter conjunct is
        // applied inside the search, before the limit is exhausted. The BE
        // paimon-rust predicate converter skips conjuncts it cannot represent (and
        // the synthetic distance column is not in its field registry at all), and
        // the engine re-applies those residuals on the returned block -- after the
        // top-K rows were already selected. A skipped residual can therefore drop
        // rows out of a bounded result: with candidate distances 0, 2 and 3, a
        // `WHERE dis > 1 ORDER BY dis LIMIT 1` would fetch only the distance-0 row,
        // filter it out and return nothing instead of the distance-2 row. So the
        // rewrite only fires when each conjunct is provably of a form the converter
        // pushes: a simple comparison / IN / null-check on a table column whose
        // Doris type is in the converter's supported set (see
        // PaimonRustPredicateConverter._is_supported_slot_type on BE). Anything
        // else -- an expression predicate, a predicate on the distance itself, an
        // unsupported column type -- declines the rewrite and the query runs as an
        // ordinary scan with the distance computed in Doris, which is always
        // correct.
        if (optionalFilter.isPresent() && !conjunctsRideIntoVectorSearch(optionalFilter.get())) {
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

        // The indexed column must be a direct slot reference. A Cast chain over the
        // column is NOT stripped: a legal cast can change vector values (for example
        // an ARRAY<FLOAT> -> ARRAY<INT> -> ARRAY<FLOAT> round trip loses precision),
        // and searching the original vectors while the query means the casted ones
        // changes both the distance and the row order. Doris's implicit coercions do
        // not produce a cast here for a matching ARRAY<FLOAT> column, so anything
        // wrapped in a Cast simply declines the rewrite.
        Expression left;
        Expression right;
        if (l2Dist) {
            left = ((L2DistanceApproximate) orderKeyExpr).left();
            right = ((L2DistanceApproximate) orderKeyExpr).right();
        } else {
            left = ((InnerProductApproximate) orderKeyExpr).left();
            right = ((InnerProductApproximate) orderKeyExpr).right();
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

        // Pre-flight the projection-dependent bounds the vector split would hit at
        // the scan node: a PK-vector split has no JNI fallback (it would return an
        // empty distance column), so a query whose projected columns would set
        // projectedVariant -- or an ORC table with TIMESTAMP_LTZ in its schema, the
        // orc-ltz-shape bound -- must not be rewritten at all. Without the rewrite
        // those queries run through the ordinary paths, where they are supported.
        try {
            if (projectedColumnsContainVariant(project, optionalFilter)
                    || orcSchemaContainsTimestampLtz(paimonTable)) {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }

        // The thrift-level enum the AnnTopNInfo will carry. expectedMetric is kept as a
        // string for the comparison above (coreOptions returns a string), but the
        // AnnTopNInfo / wire contract uses TVectorMetric, so map once here. DOT_PRODUCT
        // is the thrift enum that paimon-rust's "inner_product" score transform maps to
        // (see the BE reader's parse_score_transform).
        TVectorMetric expectedMetricEnum = l2Dist ? TVectorMetric.L2 : TVectorMetric.DOT_PRODUCT;

        // Pre-flight the rust eligibility the scan node will gate on. The rewrite is
        // one-way: a PK-vector split has no JNI fallback (the JNI reader ignores the
        // vector payload and would return a non-ANN scan with an empty distance
        // column), so the scan node can only fail loudly — a table whose gate would
        // fail (credential provider shape, REST-token IO, query-auth, merge-option
        // matrix, deletion-vector merge-on-read, scan-mode, storage scheme or HDFS
        // credentials, no rust-capable backend) must not be rewritten at all. Without
        // the rewrite the query still runs, computing the distance in Doris. The
        // projection-dependent bounds (ORC LTZ, projected variant) need the final scan
        // tuple and stay scan-node-side, and the scan node re-evaluates on the
        // relation-option-processed table, so it stays authoritative for t@options
        // overrides this raw-table pre-flight cannot see.
        try {
            Table baseTable = paimonTable.getPaimonTable(Optional.empty());
            PaimonExternalCatalog catalog = (PaimonExternalCatalog) paimonTable.getCatalog();
            Map<StorageProperties.Type, StorageProperties> storagePropertiesMap =
                    VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                            catalog.getCatalogProperty().getMetastoreProperties(),
                            catalog.getCatalogProperty().getStoragePropertiesMap(),
                            baseTable);
            Map<String, String> backendStorageProperties =
                    CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
            String tableLocation = baseTable instanceof FileStoreTable
                    ? ((FileStoreTable) baseTable).location().toString()
                    : baseTable.options().get("path");
            if (!PaimonVectorSearch.vectorScanEligible(baseTable, tableLocation,
                    backendStorageProperties,
                    Env.getCurrentSystemInfo().getAllClusterBackends(true))) {
                return null;
            }
        } catch (Throwable t) {
            return null;
        }
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

    /**
     * Whether every filter conjunct provably rides into the BE vector search (see
     * the call site for the bounded-top-N correctness argument). Conservative by
     * design: forms outside this whitelist may well be convertible, but the query
     * then simply runs un-rewritten, which is always correct.
     */
    private static boolean conjunctsRideIntoVectorSearch(LogicalFilter<?> filter) {
        for (Expression conjunct : filter.getConjuncts()) {
            if (!conjunctRidesIntoVectorSearch(conjunct)) {
                return false;
            }
        }
        return true;
    }

    private static boolean conjunctRidesIntoVectorSearch(Expression conjunct) {
        if (conjunct instanceof ComparisonPredicate) {
            ComparisonPredicate compare = (ComparisonPredicate) conjunct;
            return (isVectorFilterColumnSlot(compare.left()) && compare.right() instanceof Literal)
                    || (compare.left() instanceof Literal && isVectorFilterColumnSlot(compare.right()));
        }
        if (conjunct instanceof InPredicate) {
            InPredicate in = (InPredicate) conjunct;
            return isVectorFilterColumnSlot(in.getCompareExpr())
                    && in.getOptions().stream().allMatch(Literal.class::isInstance);
        }
        if (conjunct instanceof IsNull) {
            return isVectorFilterColumnSlot(((IsNull) conjunct).child());
        }
        if (conjunct instanceof Not && ((Not) conjunct).child() instanceof IsNull) {
            return isVectorFilterColumnSlot(((IsNull) ((Not) conjunct).child()).child());
        }
        return false;
    }

    private static boolean isVectorFilterColumnSlot(Expression expr) {
        if (!(expr instanceof SlotReference)) {
            return false;
        }
        SlotReference slot = (SlotReference) expr;
        return slot.getOriginalColumn().isPresent()
                && VECTOR_FILTER_SUPPORTED_COLUMN_TYPES.contains(
                        slot.getOriginalColumn().get().getType());
    }

    /**
     * Mirrors the scan node's projectedVariant bound (PaimonScanNode: any slot whose
     * Doris type contains a Variant keeps the query off the rust reader, and a
     * PK-vector split has no non-rust fallback): any projected or filtered column
     * containing Variant declines the rewrite so the query stays on an ordinary
     * supported path.
     */
    private static boolean projectedColumnsContainVariant(
            LogicalProject<?> project, Optional<LogicalFilter<?>> optionalFilter) {
        List<Expression> expressions = new java.util.ArrayList<>();
        expressions.addAll((project.getProjects()));
        optionalFilter.ifPresent(filter -> expressions.addAll(filter.getConjuncts()));
        for (Expression expression : expressions) {
            Set<SlotReference> slots = expression.collect(SlotReference.class::isInstance);
            for (SlotReference slotRef : slots) {
                if (!slotRef.getOriginalColumn().isPresent()) {
                    continue;
                }
                if (PaimonUtil.containsVariant(slotRef.getOriginalColumn().get().getType())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Mirrors the scan node's orc-ltz-schema bound: paimon-rust's ORC
     * TIMESTAMP_LTZ decoding is not equivalent to Doris semantics yet, and a vector
     * split would materialize rows through that decoder with no fallback. Snapshot
     * selection is irrelevant here -- this is a physical-file property.
     */
    private static boolean orcSchemaContainsTimestampLtz(PaimonExternalTable paimonTable) {
        try {
            Table baseTable = paimonTable.getPaimonTable(Optional.empty());
            if (!(baseTable instanceof FileStoreTable)) {
                return false;
            }
            FileStoreTable fileStoreTable = (FileStoreTable) baseTable;
            // Paimon's default file format is ORC; only a PARQUET table is known to
            // be free of the orc-ltz shape (an unset key falls to the default).
            String fileFormat = CoreOptions.fromMap(fileStoreTable.options()).fileFormatString();
            if (!CoreOptions.FILE_FORMAT_PARQUET.equalsIgnoreCase(String.valueOf(fileFormat))) {
                return fileStoreTable.schema().fields().stream()
                        .anyMatch(field -> containsTimestampLtz(field.type()));
            }
            return false;
        } catch (Throwable t) {
            // The table might genuinely be unreadable; the ordinary path will surface
            // the real error, and declining the rewrite is the safe answer here.
            return true;
        }
    }

    /** Mirrors PaimonScanNode.containsTimestampLtz (package-private there). */
    private static boolean containsTimestampLtz(DataType type) {
        if (type == null) {
            return false;
        }
        if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsTimestampLtz(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return containsTimestampLtz(mapType.getKeyType())
                    || containsTimestampLtz(mapType.getValueType());
        }
        if (type instanceof MultisetType) {
            return containsTimestampLtz(((MultisetType) type).getElementType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .anyMatch(field -> containsTimestampLtz(field.type()));
        }
        return false;
    }
}

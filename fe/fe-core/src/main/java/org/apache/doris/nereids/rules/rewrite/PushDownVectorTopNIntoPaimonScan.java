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
import org.apache.doris.datasource.paimon.source.PaimonScanNode;
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
import org.apache.doris.nereids.trees.expressions.functions.NoneMovableFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import java.util.HashSet;
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

    /** The BE rust predicate converter encodes the IN option list under a 16-bit
     *  child count; a bigger list is dropped whole into the post-fetch residual. */
    private static final int MAX_IN_PREDICATE_CHILDREN = 0xFFFF;

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
        //
        // `scan.creation-time-millis` must decline as well: when a snapshot exists
        // past the bound it resolves into an ordinary snapshot pin (safe), but when
        // none does — the exact read state the option exists for — PaimonScanParams
        // resolves it into the SAME internal file-creation bound (resolveOptions
        // falls through to resolveFileCreationTime), which the vector path again
        // never consults. Which case applies depends on the snapshot manager's
        // state, so the raw option key is declined unconditionally.
        if (scan.getScanParams().isPresent()) {
            TableScanParams scanParams = scan.getScanParams().get();
            if (scanParams.incrementalRead()) {
                return null;
            }
            Map<String, String> rawParams = scanParams.getMapParams();
            if (rawParams.containsKey(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key())
                    || rawParams.containsKey(CoreOptions.SCAN_CREATION_TIME_MILLIS.key())) {
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
        // A typed NULL inside the query vector ([CAST(NULL AS FLOAT), 0.0, ...]) is a
        // NullLiteral whose getDouble() returns 0, so extracting the coordinates
        // would silently search against a zero vector. The ordinary path raises
        // instead (FunctionArrayDistance::_validate_no_nulls: "Second argument ...
        // cannot have null"), and the rewrite must not replace that error with rows
        // for a different input, so any null element declines the rewrite. Both
        // supported metrics share this.
        if (((ArrayLiteral) right).getValue().stream().anyMatch(NullLiteral.class::isInstance)) {
            return null;
        }
        // A NON-FINITE coordinate (CAST('infinity' AS FLOAT), NaN) is a legal literal
        // the ordinary path supports: FunctionArrayDistance returns the row with an
        // infinite distance, but paimon-rust's PkVectorSearchParams::resolve ->
        // validate_query rejects non-finite coordinates ("must be finite"). The
        // rewrite is one-way, so it must decline and keep the ordinary execution
        // for these inputs; both metrics and NaN share this.
        if (((ArrayLiteral) right).getValue().stream().anyMatch(
                literal -> !literal.isNullLiteral()
                        && !Double.isFinite(literal.getDouble()))) {
            return null;
        }
        SlotReference vectorSlot = (SlotReference) left;
        if (!vectorSlot.getOriginalColumn().isPresent() || !vectorSlot.getOriginalTable().isPresent()) {
            return null;
        }
        String annColumnName = vectorSlot.getOriginalColumn().get().getName();

        // Validate the PK-vector index and metric on the relation's bound Paimon
        // table: a branch table carries its own index options and can differ from
        // the main table (see boundPaimonTable).
        PaimonExternalTable paimonTable = (PaimonExternalTable) scan.getTable();
        CoreOptions coreOptions;
        try {
            Table basePaimonTable = boundPaimonTable(paimonTable, scan);
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

        // A data predicate additionally requires the SDK's "physical row read"
        // table mode: the predicate itself converts fine, but the pinned
        // paimon-rust PkVectorSearchParams::resolve rejects every DATA predicate
        // unless deletion vectors are enabled and merge-on-read is disabled —
        // so the failure lands AFTER this one-way rewrite replaced the distance
        // expression, turning a query that the ordinary path answers into a
        // user-facing error. Partition-only conjuncts are enforced by the search
        // planning itself and stay fine, so decline only on a non-partition one.
        //
        // The synthetic distance slot is named after a legal paimon user column
        // name (__paimon_search_score_to_dis, see the BE reader). A table that
        // actually HAS a column of that name would collide with the synthetic
        // slot's block-column matching; decline and run the ordinary path.
        //
        // A complex-typed column (STRUCT/MAP/nested ARRAY) must ride as a whole
        // column: nested pruning rewrites a slot whose fields are accessed —
        // Project(struct_element(payload, 'b')) materializes the scan tuple as
        // ROW<b> — but the vector reader projects only root names and returns
        // the full Arrow value, and the struct SerDe then binds children by
        // ORDINAL: the wrong child comes back. Filter conjuncts cannot reach
        // this (the whitelist admits only scalar columns); the order-key alias
        // is replaced by the synthetic slot and cannot reach it either; so only
        // a Project output expression over a complex column has to decline.
        //
        // A NoneMovableFunction in the projection (assert_true) must keep
        // evaluating below the TopN — see projectContainsNonMovableExpression.
        //
        // A historically evolved schema must stay off the vector reader: its
        // materialization decodes projected values through Arrow safe casts,
        // which can silently NULL out-of-range values (a TIMESTAMP(3) widened
        // to TIMESTAMP(9) reading a year-2300 value) where the ordinary path
        // preserves them — see PaimonRustReaderCapabilities.hasRustIncompatibleSchemaHistory.
        try {
            Table table = boundPaimonTable(paimonTable, scan);
            if ((optionalFilter.isPresent()
                    && filterHasDataConjunct(optionalFilter.get(), table)
                    && !PaimonScanNode.vectorSearchSupportsDataPredicate(table))
                    || table.rowType().getFieldNames().stream().anyMatch(
                            name -> name.equalsIgnoreCase(PaimonVectorSearch.SEARCH_DISTANCE_COLUMN)
                                    || name.equalsIgnoreCase("__paimon_search_score")
                                    || name.equalsIgnoreCase("_PKEY_VECTOR_POSITION"))
                    || !complexColumnsRideAsWholeColumns(project, orderKeyAlias, orderKeyExpr)
                    || projectContainsNonMovableExpression(project, orderKeyAlias)
                    || (table instanceof FileStoreTable
                            && PaimonVectorSearch.hasRustIncompatibleSchemaHistory(
                                    (FileStoreTable) table))
                    || projectionNeedsNanosecondReader(project, optionalFilter)) {
                return null;
            }
        } catch (Throwable t) {
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
                    || orcSchemaContainsTimestampLtz(boundPaimonTable(paimonTable, scan))) {
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
            Table baseTable = boundPaimonTable(paimonTable, scan);
            PaimonExternalCatalog catalog = (PaimonExternalCatalog) paimonTable.getCatalog();
            // data-file.external-paths storage: a resolving-file-io catalog stores
            // data across backends Java dispatches per URI, but paimon-rust opens
            // the table through a single FileIO and rejects foreign schemes (an
            // oss:// external path on an S3 table). The per-split external gate
            // stays scan-node-side; this pre-flight declines the whole rewrite
            // when the catalog (or the table's options) opts into that layout.
            if ("true".equalsIgnoreCase(catalog.getCatalogProperty().getOrDefault(
                            "paimon.resolving-file-io.enabled", "false"))
                    || "true".equalsIgnoreCase(baseTable.options().getOrDefault(
                            "resolving-file-io.enabled", "false"))) {
                return null;
            }
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
        // NOTE: the FE-side data-predicate interception is intentionally MINIMAL.
        // The data predicate is handed in full to the BE via the vector search
        // builder's residual filter, where the Rust candidate search stage applies
        // it together with deletion vectors. The two planner-side exceptions above
        // (unridable conjunct shapes, and the SDK's no-deletion-vector table mode)
        // exist because those failures would surface only AFTER the one-way rewrite
        // replaced the distance expression. Any other table shape Rust cannot
        // handle correctly is Rust's responsibility to reject or answer correctly,
        // not the planner's.

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
            return (isVectorFilterColumnSlot(compare.left()) && ridesAsLiteral(compare.right()))
                    || (ridesAsLiteral(compare.left()) && isVectorFilterColumnSlot(compare.right()));
        }
        if (conjunct instanceof InPredicate) {
            InPredicate in = (InPredicate) conjunct;
            // The BE converter encodes the option list under a 16-bit child count
            // (see PaimonRustPredicateConverter._convert_in): a bigger list —
            // reachable by raising expr_children_limit — is dropped whole and lands
            // in the post-fetch residual, re-enabling the bounded-TopK truncation
            // this whitelist exists to prevent. Mirror the converter's cap: the
            // slot plus the options must fit uint16.
            if (in.getOptions().size() + 1 > MAX_IN_PREDICATE_CHILDREN) {
                return false;
            }
            return isVectorFilterColumnSlot(in.getCompareExpr())
                    && in.getOptions().stream().allMatch(PushDownVectorTopNIntoPaimonScan::ridesAsLiteral);
        }
        if (conjunct instanceof IsNull) {
            return isVectorFilterColumnSlot(((IsNull) conjunct).child());
        }
        if (conjunct instanceof Not && ((Not) conjunct).child() instanceof IsNull) {
            return isVectorFilterColumnSlot(((IsNull) ((Not) conjunct).child()).child());
        }
        return false;
    }

    /**
     * A literal the BE paimon-rust predicate converter accepts. A {@link NullLiteral}
     * is a Literal and passes the earlier instanceof check, but
     * {@code _convert_literal} rejects it ({@code is_null_at(0)}), which rejects the
     * WHOLE predicate — an IN option of {@code tag IN (1, 2, NULL)} silently rejects
     * the entire IN, leaving it in the post-fetch residual. That reintroduces the
     * bounded-TopK truncation the whitelist exists to prevent, so a null literal
     * never rides.
     */
    private static boolean ridesAsLiteral(Expression expr) {
        return expr instanceof Literal && !(expr instanceof NullLiteral);
    }

    /**
     * Mirror of the scan node's nanosecond-reader gate: a slot whose type
     * recursively contains a TIMESTAMP_NS column stays on JNI (the reader
     * family that preserves INT96/ORC nanos, nested values included), and a
     * rewritten vector split has no JNI fallback — so a projection or filter
     * referencing one must decline the rewrite. The scan node checks the desc
     * slots; here the same slots are the ones the Project and Filter reference.
     * The schema-history gate does not subsume this: a table can carry
     * TIMESTAMP(9) columns in its CURRENT schema with no evolution history.
     */
    private static boolean projectionNeedsNanosecondReader(
            LogicalProject<?> project, Optional<LogicalFilter<?>> optionalFilter) {
        Set<SlotReference> slots = new HashSet<>();
        project.getProjects().forEach(
                p -> slots.addAll(p.collect(SlotReference.class::isInstance)));
        optionalFilter.ifPresent(filter -> filter.getConjuncts().forEach(
                conjunct -> slots.addAll(conjunct.collect(SlotReference.class::isInstance))));
        for (SlotReference slot : slots) {
            if (slot.getOriginalColumn().isPresent()
                    && PaimonUtil.containsTimestampNs(slot.getOriginalColumn().get().getType())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the filter carries a conjunct on a non-partition column — the DATA
     * half of the predicate, the half the paimon-rust vector search applies per
     * candidate row. Every conjunct already passed the riding whitelist, so each
     * is a simple comparison / IN / null-check whose slots map to original table
     * columns; a conjunct touching only partition columns is enforced by split
     * planning and needs no per-row filtering.
     */
    private static boolean filterHasDataConjunct(LogicalFilter<?> filter, Table paimonTable) {
        Set<String> partitionKeys = new HashSet<>(paimonTable.partitionKeys());
        for (Expression conjunct : filter.getConjuncts()) {
            Set<SlotReference> slots = conjunct.collect(SlotReference.class::isInstance);
            for (SlotReference slot : slots) {
                if (slot.getOriginalColumn().isPresent()
                        && !partitionKeys.contains(slot.getOriginalColumn().get().getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The relation's bound Paimon table -- branch, time travel, statement fence or
     * option overrides included -- instead of the cached main table. A branch is an
     * independently configured table: it can carry a different vector-index metric
     * (inner-product where main has l2), different deletion-vector options or a
     * different schema, and the scan reads the BRANCH's schema through paimon-rust.
     * Validating index/metric/eligibility against the main table would accept a
     * query whose search then scores and reorders with the branch's metric --
     * wrong results even at full recall.
     *
     * <p>Relation options ({@code t@options(...)}) apply over the statement snapshot
     * and are resolved through the table's own option-application path; everything
     * else rides the relation's bound snapshot captured at binding.
     */
    private static Table boundPaimonTable(PaimonExternalTable paimonTable, LogicalFileScan scan) {
        if (scan.getScanParams().isPresent() && scan.getScanParams().get().isOptions()) {
            return paimonTable.getPaimonTable(scan.getScanParams().get());
        }
        return paimonTable.getPaimonTable(scan.getRelationSnapshot());
    }

    /**
     * A Project expression whose evaluation must stay BELOW the TopN (a
     * NoneMovableFunction, such as {@code assert_true}) still has to raise on rows
     * the bounded Top-K would never fetch: on the ordinary path the Project
     * evaluates before the TopN and fails for a rejected far row, while
     * {@code withVectorTopN} truncates the scan to the near rows first, so the
     * assertion never sees them and the query silently succeeds. Any non-movable
     * expression outside the order-key alias keeps the ordinary path. This is a
     * SELECT expression; the filter residual thread is separate.
     */
    private static boolean projectContainsNonMovableExpression(
            LogicalProject<?> project, Alias orderKeyAlias) {
        for (NamedExpression projection : project.getProjects()) {
            if (!(projection instanceof Alias) || projection.equals(orderKeyAlias)) {
                continue;
            }
            if (((Alias) projection).child().anyMatch(e -> e instanceof NoneMovableFunction)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether every complex-typed column referenced by the projection rides as a
     * whole column. Nested pruning materializes the scan tuple with a REDUCED type
     * for a column whose fields are accessed ({@code Project(struct_element(payload,
     * 'b'))} prunes {@code payload ROW<a,b>} to {@code ROW<b>}), but the vector
     * reader projects only root names and returns the full Arrow value, whose
     * struct SerDe binds children by ordinal — the wrong child comes back. A bare
     * slot output keeps the full type — UNLESS it was already pruned (an outer
     * field projection can leave an already-pruned bare slot here), whose reduced
     * type no longer equals the original column's type; the desc-side backstop
     * would fail loudly, so decline instead. A complex slot consumed ONLY by a
     * distance function never reaches the scan at all (this rewrite replaces
     * every occurrence of the function with the synthetic distance slot,
     * order-key alias included), so only any OTHER expression over a complex
     * column forces the decline. Filter conjuncts cannot reach this: the shape
     * whitelist admits only scalar columns.
     */
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
     *
     * <p>The check is per PHYSICAL FILE, not per current format: a table written
     * as ORC with TIMESTAMP_LTZ and later switched to Parquet (retaining the ORC
     * rows) still exposes those rows to setPaimonParams' ORC detection, so a
     * currently-Parquet table declines when any schema era declared ORC and any
     * schema era (its own fields, or the current ones) carried LTZ columns.
     */
    private static boolean orcSchemaContainsTimestampLtz(Table boundTable) {
        try {
            Table baseTable = boundTable;
            if (!(baseTable instanceof FileStoreTable)) {
                return false;
            }
            FileStoreTable fileStoreTable = (FileStoreTable) baseTable;
            // Paimon's default file format is ORC; only a PARQUET declaration is
            // known to be free of the orc shape (an unset key falls to the default).
            String fileFormat = CoreOptions.fromMap(fileStoreTable.options()).fileFormatString();
            boolean currentOrc = !CoreOptions.FILE_FORMAT_PARQUET.equalsIgnoreCase(
                    String.valueOf(fileFormat));
            if (currentOrc) {
                return fileStoreTable.schema().fields().stream()
                        .anyMatch(field -> containsTimestampLtz(field.type()));
            }
            // Currently Parquet: files are shaped by the schema era that wrote
            // them, so an ORC era's retained files still reach the reader. Any
            // historical schema that declared ORC AND carried LTZ columns (in its
            // own fields or in the current schema, which those rows must also
            // decode under) forces the decline.
            for (TableSchema historical : fileStoreTable.schemaManager().listAll()) {
                if (historical == null || historical.id() == fileStoreTable.schema().id()) {
                    continue;
                }
                String eraFormat = CoreOptions.fromMap(historical.options()).fileFormatString();
                if (CoreOptions.FILE_FORMAT_PARQUET.equalsIgnoreCase(String.valueOf(eraFormat))) {
                    continue;
                }
                if (historical.fields().stream().anyMatch(field -> containsTimestampLtz(field.type()))
                        || fileStoreTable.schema().fields().stream()
                                .anyMatch(field -> containsTimestampLtz(field.type()))) {
                    return true;
                }
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

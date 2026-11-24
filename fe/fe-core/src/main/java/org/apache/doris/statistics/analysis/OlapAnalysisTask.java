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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.analysis.FixedBlockSampleSpec.Mode;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.repository.StatisticsRepository;
import org.apache.doris.statistics.repository.StatsId;
import org.apache.doris.statistics.sampling.ColumnPhysicalLayout;
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.LegacyTabletSamplingMethod;
import org.apache.doris.statistics.sampling.RandomAggTabletSamplingMethod;
import org.apache.doris.statistics.sampling.SampledScanSqlRenderer;
import org.apache.doris.statistics.sampling.SamplingBudget;
import org.apache.doris.statistics.sampling.TabletSampleSpec;
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FullScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.LegacySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSamplingMethod;
import org.apache.doris.statistics.sampling.TabletSamplingRequest;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.text.StringSubstitutor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

/**
 * Each task analyze one column.
 */
public class OlapAnalysisTask extends BaseAnalysisTask {

    static final int MAX_INTERNAL_SQL_BYTES = 1024 * 1024;

    // Assigned only to table-sample tasks when the job is constructed.
    ColumnMinMaxBatchCollector.Batch minMaxBatch;

    private boolean fullScanOfNonNullUniqueKey;
    // Set only for a logical sample returning raw moments. A census follows freezeResultCount's census path.
    private double logicalSampleProbability;
    private boolean logicalSampleUnique;
    private double logicalSampleDomainRate = 1.0;

    @VisibleForTesting
    public OlapAnalysisTask() {
    }

    public OlapAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    public void doExecute() throws Exception {
        if (killed) {
            return;
        }
        if (tableSample != null && info.rowCount == 0) {
            OlapTable table = (OlapTable) tbl;
            if (!IndexSamplingSnapshot.hasWrittenPartition(table)) {
                StatsId id = new StatsId(concatColumnStatsId(), info.catalogId, info.dbId,
                        info.tblId, info.indexId, info.colName, null);
                long frozen = job.freezeIndexRows(targetIndexId(), () -> 0);
                if (frozen != 0) {
                    throw new AnalyzeSkipException("Index became empty during statistics collection");
                }
                job.getJobInfo().addIndexRowCount(targetIndexId(), frozen);
                job.appendBuf(this, Collections.singletonList(new ColStatsData(id)));
                return;
            }
            // A zero table count can precede the first tablet report, particularly on Cloud.
            // Fall through to the per-index check in doSample (the base table count may be stale).
        }
        if (tableSample != null) {
            doSample();
        } else {
            if (info.jobType == AnalysisInfo.JobType.SYSTEM) {
                job.requireIndexReports((OlapTable) tbl, targetIndexId());
            }
            doFull();
        }
        LOG.info("AnalysisTask Done {}", this.toString());
    }

    @Override
    protected void doSample() {
        logicalSampleProbability = 0;
        logicalSampleDomainRate = 1.0;
        fullScanOfNonNullUniqueKey = false;
        OlapTable table = (OlapTable) tbl;
        long indexId = targetIndexId();
        long rows = job.reportedIndexRows(table, indexId);
        if (rows <= 0 && IndexSamplingSnapshot.hasWrittenPartition(table)) {
            throw new AnalyzeSkipException("Written partitions have no positive index row count; deferring statistics");
        }
        if ((info.jobType == AnalysisInfo.JobType.SYSTEM || info.rowCount == 0) && rows > 0) {
            job.requireIndexReports(table, indexId);
        }
        KeysType keys;
        boolean mow;
        boolean hasRowTtl;
        boolean retainsPartitionKeys;
        boolean legacyPreAgg;
        ColumnPhysicalLayout layout;
        List<String> keyColumnNames;
        table.readLock();
        try {
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
            Column physical = meta == null ? null : resolveColumn(meta, info.colName);
            if (physical == null) {
                throw new AnalyzeSkipException("Statistics target column or index was removed: " + info.colName);
            }
            col = physical;
            keys = meta.getKeysType();
            mow = table.getEnableUniqueKeyMergeOnWrite();
            hasRowTtl = table.hasRowTtl();
            retainsPartitionKeys = indexId == table.getBaseIndexId() || FixedBlockSampleSpec.retainsPartitionKeys(
                    table, meta);
            legacyPreAgg = !hasRowTtl && (table.getKeysType() == KeysType.AGG_KEYS
                    || table.getKeysType() == KeysType.UNIQUE_KEYS && !mow);
            layout = columnLayout(table, meta, physical);
            keyColumnNames = keys == KeysType.AGG_KEYS
                    ? meta.getSchema().stream().filter(Column::isKey).map(Column::getName).toList()
                    : List.of();
        } finally {
            table.readUnlock();
        }
        TabletSamplingMethod method = chooseTabletSamplingMethod(table, keys, mow, layout, rows);
        boolean fixed = method instanceof HierarchicalFixedBudgetTabletSamplingMethod
                || method instanceof RandomAggTabletSamplingMethod;
        if (!fixed && !col.isKey() && (keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS && !mow)) {
            throw new AnalyzeSkipException("Legacy sample cannot collect unmerged value column: " + info.colName);
        }
        if (!fixed && info.jobType == AnalysisInfo.JobType.SYSTEM
                && IndexSamplingSnapshot.reportedIndexRows(table, indexId, true) == TableIf.UNKNOWN_ROW_COUNT) {
            throw new AnalyzeSkipException("Legacy automatic sampling needs a complete index row count report");
        }
        boolean logicalSample = fixed && (keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS || hasRowTtl);
        boolean inSampleBounds = fixed && !col.isKey()
                && (keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS && !mow);
        boolean singleUniqueKey = layout.singleUniqueKey() && retainsPartitionKeys;
        boolean singleNonNullUnique = singleUniqueKey && !col.isAllowNull();
        Map<String, String> params = buildSqlParams();
        params.put("limit", "");
        params.put("sampleHints", "");
        params.put("sampleWhere", "");
        params.put("rowCount", Long.toString(rows));
        TabletSampleSpec spec = buildSampleSpec(method, table, indexId, layout, rows, params,
                singleNonNullUnique, logicalSample, inSampleBounds, keyColumnNames);
        if (!fixed && legacyPreAgg) {
            params.put("preAggHint", "/*+PREAGGOPEN*/");
        }
        if (spec.scanClause() instanceof FullScanClause) {
            fullScanOfNonNullUniqueKey = singleNonNullUnique;
            params.put("rowCount", "COUNT(1)");
            params.put("min", "MIN(${colName})");
            params.put("max", "MAX(${colName})");
            String ndv = fixed ? "LEAST(COUNT(${colName}), NDV(${colName}))" : "NDV(${colName})";
            params.put("ndvFunction", (fixed ? singleNonNullUnique : singleUniqueKey) ? "COUNT(1)" : ndv);
            params.put("scaleFactor", "1");
            params.put("dataSizeFunction", getDataSizeFunction(col, false));
            runQuery(SampledScanSqlRenderer.checked(new StringSubstitutor(params).replace(LINEAR_ANALYZE_TEMPLATE),
                    MAX_INTERNAL_SQL_BYTES));
            return;
        }
        if (!inSampleBounds) {
            ResultRow bounds = getSampleMinMax();
            params.put("min", boundLiteral(bounds.get(0), fixed));
            params.put("max", boundLiteral(bounds.get(1), fixed));
        }
        params.put("sampleHints", SampledScanSqlRenderer.render(spec.scanClause(), MAX_INTERNAL_SQL_BYTES));
        params.put("sampleWhere", SampledScanSqlRenderer.renderKeyHashWhere(spec.scanClause(), keyColumnNames));
        if (logicalSample) {
            EqualProbabilitySamplingSemantics sem = (EqualProbabilitySamplingSemantics) spec.semantics();
            logicalSampleProbability = sem.probability();
            logicalSampleUnique = singleNonNullUnique;
            logicalSampleDomainRate = subdomainRate(layout, singleUniqueKey, sem);
            runQuery(SampledScanSqlRenderer.checked(TableStatisticsSqlBuilder.sampleSql(params, singleNonNullUnique,
                    true, inSampleBounds), MAX_INTERNAL_SQL_BYTES));
            return;
        }
        long population = job.freezeIndexRows(indexId, () -> rows);
        if (population <= 0) {
            throw new AnalyzeSkipException("Index row count is not ready for statistics sampling");
        }
        params.put("rowCount", Long.toString(population));
        String sql;
        if (spec.semantics() instanceof LegacySamplingSemantics legacy) {
            params.put("limit", legacy.limit() > 0 ? "limit " + legacy.limit() : "");
            params.put("scaleFactor", Double.toString(legacy.scaleFactor() * ((double) population / rows)));
            if (legacy.linear()) {
                params.put("ndvFunction", singleUniqueKey ? "${rowCount}"
                        : "ROUND(NDV(${colName}) * ${scaleFactor})");
            } else {
                setGroupedParams(params);
                params.put("ndvFunction", getNdvFunction(Long.toString(population)));
            }
            String template = legacy.linear() ? LINEAR_ANALYZE_TEMPLATE
                    : TableStatisticsSqlBuilder.DUJ1_ANALYZE_TEMPLATE;
            sql = new StringSubstitutor(params).replace(template);
        } else {
            EqualProbabilitySamplingSemantics sem = (EqualProbabilitySamplingSemantics) spec.semantics();
            params.put("domainRate", Double.toString(subdomainRate(layout, singleUniqueKey, sem)));
            sql = TableStatisticsSqlBuilder.fixed(params, singleNonNullUnique);
        }
        runQuery(SampledScanSqlRenderer.checked(sql, MAX_INTERNAL_SQL_BYTES));
    }

    private TabletSampleSpec buildSampleSpec(TabletSamplingMethod method, OlapTable table, long indexId,
            ColumnPhysicalLayout layout, long rows, Map<String, String> params, boolean singleNonNullUnique,
            boolean logicalSample, boolean inSampleBounds, List<String> keyColumnNames) {
        boolean fixed = method instanceof HierarchicalFixedBudgetTabletSamplingMethod
                || method instanceof RandomAggTabletSamplingMethod;
        int clauseBytes = MAX_INTERNAL_SQL_BYTES;
        if (fixed) {
            params.put("rawDataSizeFunction", params.get("dataSizeFunction"));
            setGroupedParams(params);
            // PR1 bounds are truncated to 1024 characters each. Reserve four UTF-8 bytes per
            // character (also covers doubled quotes/backslashes) and the enclosing SQL quotes.
            params.put("min", "NULL");
            params.put("max", "NULL");
            params.put("domainRate", "0.00000000000000001");
            int whereReserve = method instanceof RandomAggTabletSamplingMethod
                    ? SampledScanSqlRenderer.renderKeyHashWhere(
                            new KeyHashScanClause(Optional.empty(), Long.MAX_VALUE, 1), keyColumnNames)
                            .getBytes(StandardCharsets.UTF_8).length
                    : 0;
            int boundsReserve = inSampleBounds ? 0 : 2 * (4096 + 2);
            clauseBytes -= TableStatisticsSqlBuilder.sampleSql(params, singleNonNullUnique, logicalSample,
                    inSampleBounds).getBytes(StandardCharsets.UTF_8).length + boundsReserve + whereReserve;
            params.remove("domainRate");
        }
        SamplingBudget budget = new SamplingBudget(targetSampleRows(rows, fixed),
                StatisticsUtil.getStatisticsSampleUnitRows(),
                StatisticsUtil.getStatisticsMaxSampleTablets(), 4, clauseBytes);
        TabletSamplingScope scope = fixed ? job.samplingFrame(table, indexId).scope()
                : TabletSamplingScope.table(List.of());
        TabletSamplingRequest request = new TabletSamplingRequest(scope, layout, budget, rows,
                job.samplingSeed(tableSample.getSeek()));
        return method.build(request);
    }

    static double subdomainRate(ColumnPhysicalLayout layout, boolean singleUniqueKey,
            EqualProbabilitySamplingSemantics sem) {
        if (singleUniqueKey) {
            return 1.0;
        }
        double rate = 1.0;
        if (layout.partition()) {
            rate *= sem.partitionRate();
        }
        if (layout.distribution()) {
            rate *= sem.bucketRate();
        }
        return rate > 0 && rate < 1.0 ? rate : 1.0;
    }

    private long targetSampleRows(long rows, boolean fixed) {
        return fixed && tableSample.isPercent() ? Math.max(1, (long) (rows * (tableSample.getSampleValue() / 100.0)))
                : getSampleRows();
    }

    private static String boundLiteral(String value, boolean fixed) {
        return fixed && value == null ? "NULL" : StatisticsUtil.quote(StatisticsUtil.escapeSQL(value));
    }

    static Column resolveColumn(MaterializedIndexMeta meta, String colName) {
        Column column = meta.getColumnByName(colName);
        if (column == null || column.getName().equalsIgnoreCase(colName)) {
            return column;
        }
        // The catalog lookup strips embedded backticks. SQL reads the literal identifier.
        return meta.getSchema().stream().filter(c -> c.getName().equalsIgnoreCase(colName)).findFirst().orElse(null);
    }

    static ColumnPhysicalLayout columnLayout(OlapTable table, MaterializedIndexMeta meta, Column column) {
        Set<String> distribution = table.getDistributionColumnNames();
        boolean singleUniqueKey = column.isKey()
                && (meta.getKeysType() == KeysType.UNIQUE_KEYS || meta.getKeysType() == KeysType.AGG_KEYS)
                && meta.getSchema().stream().filter(Column::isKey).count() == 1;
        boolean singleBucketColumn = distribution.size() == 1
                && distribution.contains(column.tryGetBaseColumnName().toLowerCase(Locale.ROOT));
        return new ColumnPhysicalLayout(column.isKey(), table.isPartitionColumn(column),
                singleUniqueKey, singleBucketColumn);
    }

    private long targetIndexId() {
        return info.indexId == -1 ? ((OlapTable) tbl).getBaseIndexId() : info.indexId;
    }

    private void setGroupedParams(Map<String, String> params) {
        params.put("subStringColName", getStringTypeColName(col));
        params.put("dataSizeFunction", getDataSizeFunction(col, true));
        params.put("hotValueCollectCount", Integer.toString(SessionVariable.getHotValueCollectCount()));
    }

    TabletSamplingMethod chooseTabletSamplingMethod(OlapTable table, KeysType keys, boolean mow,
            ColumnPhysicalLayout layout, long rows) {
        if (StatisticsUtil.enableFixedBlockStatsSampling() && rows > 0
                && (keys == KeysType.DUP_KEYS || keys == KeysType.UNIQUE_KEYS && mow)
                && job.samplingFrame(table, targetIndexId()).allCandidatesSupportFixedBlock()) {
            return new HierarchicalFixedBudgetTabletSamplingMethod();
        }
        if ((keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS && !mow)
                && job.useKeyRange(targetIndexId(), () -> keyRangeEligible(table, rows))) {
            return new HierarchicalFixedBudgetTabletSamplingMethod(Mode.KEY_RANGE_BLOCK);
        }
        if (keys == KeysType.AGG_KEYS
                && job.useRandomAgg(targetIndexId(), () -> randomAggEligible(table, rows))) {
            return new RandomAggTabletSamplingMethod(
                    job.samplingFrame(table, targetIndexId()).allCandidatesSupportFixedBlock());
        }
        return new LegacyTabletSamplingMethod(table, info.indexId, tableSample.getSeek());
    }

    private boolean keyRangeEligible(OlapTable table, long rows) {
        long id = targetIndexId();
        if (!StatisticsUtil.enableFixedBlockStatsSampling() || rows <= 0) {
            return false;
        }
        table.readLock();
        try {
            if (!FixedBlockSampleSpec.supportsKeyRange(table, id)) {
                return false;
            }
        } finally {
            table.readUnlock();
        }
        return job.samplingFrame(table, id).allCandidatesSupport(true);
    }

    private boolean randomAggEligible(OlapTable table, long rows) {
        long id = targetIndexId();
        if (!StatisticsUtil.enableFixedBlockStatsSampling() || rows <= 0) {
            return false;
        }
        table.readLock();
        try {
            return FixedBlockSampleSpec.supportsRandomAgg(table, id);
        } finally {
            table.readUnlock();
        }
    }

    ResultRow freezeResultCount(ResultRow row) {
        if (logicalSampleProbability > 0) {
            return freezeLogicalSample(row, job, targetIndexId(), logicalSampleProbability, logicalSampleUnique,
                    logicalSampleDomainRate);
        }
        String count = row.getWithDefault(7, "0");
        long measured = roundedCount(count);
        if (measured == -1) {
            throw new AnalyzeSkipException("Fixed-block sample has zero visible rows; preserving column statistics");
        }
        long frozen = job.freezeIndexRows(targetIndexId(), () -> measured);
        if (measured == frozen) {
            if (Long.toString(frozen).equals(count)) {
                return row;
            }
            // Publish the same rounded integer in column_statistics and jobInfo.
            List<String> values = new ArrayList<>(row.getValues());
            values.set(7, Long.toString(frozen));
            return new ResultRow(values);
        }
        if (measured == 0 || frozen == 0) {
            throw new AnalyzeSkipException("Index changed between empty and nonempty during statistics collection");
        }
        // A census can see a concurrent write after another column froze N. Sample queries already
        // return N. Bring census totals onto the same population before publishing the row.
        List<String> values = new ArrayList<>(row.getValues());
        long nulls = Math.min(frozen, scaleCensusTotal(row.getWithDefault(9, "0"), measured, frozen));
        long distinct = roundedCount(row.getWithDefault(8, "0"));
        values.set(7, Long.toString(frozen));
        values.set(8, Long.toString(fullScanOfNonNullUniqueKey ? frozen : Math.min(distinct, frozen - nulls)));
        values.set(9, Long.toString(nulls));
        values.set(12, Long.toString(scaleCensusTotal(row.getWithDefault(12, "0"), measured, frozen)));
        return new ResultRow(values);
    }

    static ResultRow freezeLogicalSample(ResultRow row, AnalysisJob job, long indexId, double q, boolean unique) {
        return freezeLogicalSample(row, job, indexId, q, unique, 1.0);
    }

    static ResultRow freezeLogicalSample(ResultRow row, AnalysisJob job, long indexId, double q, boolean unique,
            double domainRate) {
        long n = Long.parseLong(row.get(7));
        if (n == 0) {
            throw new AnalyzeSkipException("Logical sample has zero visible rows; preserving statistics");
        }
        long z = Long.parseLong(row.get(9));
        long d = Long.parseLong(row.get(8));
        long singletons = Long.parseLong(row.get(15));
        BigDecimal bytes = new BigDecimal(row.get(12));
        long m = n - z;
        Preconditions.checkState(n > 0 && z >= 0 && z <= n && d >= 0 && d <= m && singletons >= 0
                && singletons <= d && d - singletons <= m - d && (d > singletons || m == d)
                && bytes.signum() >= 0 && (!unique || z == 0 && d == n));
        long estimate = BigDecimal.valueOf(n).divide(BigDecimal.valueOf(q), 0, RoundingMode.HALF_UP).longValueExact();
        long candidateBytes = scaledTotal(bytes, n, estimate).longValueExact();
        long frozen = job.freezeIndexRows(indexId, () -> estimate);
        if (frozen == 0) {
            throw new AnalyzeSkipException("Index changed between empty and nonempty during statistics collection");
        }
        BigDecimal f = BigDecimal.valueOf(frozen);
        long nulls = scaleCensusTotal(Long.toString(z), n, frozen);
        long ndv = 0;
        if (unique) {
            ndv = frozen;
        } else if (d > 0) {
            BigDecimal nonNull = BigDecimal.valueOf(m);
            if (domainRate > 0 && domainRate < 1.0) {
                BigDecimal rate = BigDecimal.valueOf(domainRate);
                BigDecimal visited = nonNull.max(f.multiply(rate));
                BigDecimal denom = BigDecimal.valueOf(m - singletons).multiply(visited)
                        .add(BigDecimal.valueOf(singletons).multiply(nonNull));
                BigDecimal scaled = nonNull.multiply(BigDecimal.valueOf(d)).multiply(visited)
                        .divide(denom.multiply(rate), 0, RoundingMode.HALF_UP);
                ndv = scaled.max(BigDecimal.valueOf(d)).min(BigDecimal.valueOf(frozen - nulls)).longValueExact();
            } else {
                BigDecimal divisor = BigDecimal.valueOf(m - singletons).multiply(f)
                        .add(BigDecimal.valueOf(singletons).multiply(nonNull));
                ndv = nonNull.multiply(BigDecimal.valueOf(d)).multiply(f).divide(divisor, 0, RoundingMode.HALF_UP)
                        .min(BigDecimal.valueOf(frozen - nulls)).longValueExact();
            }
        }
        List<String> values = new ArrayList<>(row.getValues().subList(0, 15));
        values.set(7, Long.toString(frozen));
        values.set(8, Long.toString(ndv));
        values.set(9, Long.toString(nulls));
        long finalBytes = frozen == estimate ? candidateBytes : scaledTotal(bytes, n, frozen).longValueExact();
        values.set(12, Long.toString(finalBytes));
        return new ResultRow(values);
    }

    private static long roundedCount(String value) {
        return new BigDecimal(value).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    private static long scaleCensusTotal(String total, long measured, long frozen) {
        return scaledTotal(new BigDecimal(total), measured, frozen).min(BigDecimal.valueOf(Long.MAX_VALUE))
                .longValueExact();
    }

    private static BigDecimal scaledTotal(BigDecimal total, long measured, long frozen) {
        return total.multiply(BigDecimal.valueOf(frozen)).divide(BigDecimal.valueOf(measured), 0, RoundingMode.HALF_UP);
    }

    ResultRow getSampleMinMax() {
        checkMinMaxCancelled();
        if (minMaxBatch != null) {
            try {
                ResultRow bounds = minMaxBatch.get(this);
                if (bounds != null) {
                    return bounds;
                }
            } catch (InterruptedException e) {
                // Mark just this task cancelled before the async wrapper observes the interruption.
                // Restore the interrupt after updating task metadata.
                Thread.interrupted();
                try {
                    checkMinMaxCancelled();
                    cancel();
                } finally {
                    Thread.currentThread().interrupt();
                }
                throw new CancellationException("Interrupted collecting analysis bounds");
            }
        }
        return collectMinMax();
    }

    void checkMinMaxCancelled() {
        if (killed || (job != null && job.isKilled())) {
            throw new CancellationException("Analysis task cancelled");
        }
    }

    String qualifiedTableName() {
        return SqlUtils.getIdentSql(catalog.getName()) + "." + SqlUtils.getIdentSql(db.getFullName())
                + "." + SqlUtils.getIdentSql(tbl.getName());
    }

    static String buildMinMaxSql(String qualifiedTable, String indexSql, List<String> columns) {
        String projection = columns.stream().map(SqlUtils::getIdentSql).map(name ->
                "SUBSTRING(CAST(MIN(" + name + ") AS STRING), 1, 1024), "
                        + "SUBSTRING(CAST(MAX(" + name + ") AS STRING), 1, 1024)").collect(Collectors.joining(", "));
        return "SELECT " + projection + " FROM " + qualifiedTable + " " + indexSql;
    }

    static ResultRow boundsRow(List<ResultRow> rows, int columns) {
        Preconditions.checkState(rows.size() == 1 && rows.get(0).getValues().size() == 2 * columns,
                "Expected one bounds row with %s values", 2 * columns);
        return rows.get(0);
    }

    @Override
    protected void cancelQueries() {
        if (minMaxBatch != null) {
            minMaxBatch.onTaskCancelled();
        }
        super.cancelQueries();
    }

    protected ResultRow collectMinMax() {
        checkMinMaxCancelled();
        long startTime = System.currentTimeMillis();
        String sql = buildMinMaxSql(qualifiedTableName(), getIndex(), Collections.singletonList(info.colName));
        try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
            stmtExecutor = new StmtExecutor(r.connectContext, sql);
            checkMinMaxCancelled();
            List<ResultRow> rows = stmtExecutor.executeInternalQuery();
            checkIndividualQueryCancelled();
            ResultRow row = boundsRow(rows, 1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cost time in millisec: " + (System.currentTimeMillis() - startTime) + " Min max SQL: "
                        + sql + " QueryId: " + DebugUtil.printId(stmtExecutor.getContext().queryId()));
            }
            return row;
        } catch (RuntimeException e) {
            checkIndividualQueryCancelled();
            LOG.info("Failed to collect basic stat {}. Reason {}", sql, e.getMessage());
            throw e;
        } finally {
            stmtExecutor = null;
        }
    }

    private void checkIndividualQueryCancelled() {
        Status reason = stmtExecutor == null ? null : stmtExecutor.getPendingCancelReason();
        if (reason != null) {
            throw new CancellationException(reason.getErrorMsg());
        }
        checkMinMaxCancelled();
    }

    protected void doFull() throws Exception {
        logicalSampleProbability = 0;
        logicalSampleDomainRate = 1.0;
        fullScanOfNonNullUniqueKey = false;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do full collection for column {}", col.getName());
        }
        if (StatisticsUtil.enablePartitionAnalyze() && tbl.isPartitionedTable()) {
            doPartitionTable();
        } else {
            Map<String, String> params = buildSqlParams();
            StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
            if (shouldCollectHotValue()) {
                params.put("hotValueCollectCount", String.valueOf(SessionVariable.getHotValueCollectCount()));
                params.put("subStringColName", getStringTypeColName(col));
                runQuery(stringSubstitutor.replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE));
            } else {
                runQuery(stringSubstitutor.replace(FULL_ANALYZE_WITHOUT_HOT_VALUE_TEMPLATE));
            }
        }
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
        TableStatsMeta tableStats = Env.getServingEnv().getAnalysisManager().findTableStatsStatus(tbl.getId());
        if (tableStats == null) {
            return;
        }
        OlapTable table = (OlapTable) tbl;
        List<Long> dropped;
        ColStatsMeta columnStats;
        table.readLock();
        try {
            String indexName = info.indexId == -1 ? table.getName() : table.getIndexNameById(info.indexId);
            columnStats = tableStats.findColumnStatsMeta(indexName, info.colName);
            if (columnStats == null || columnStats.partitionUpdateRows == null
                    || columnStats.partitionUpdateRows.isEmpty()) {
                return;
            }
            if (!tableStats.partitionChanged.get()
                    && columnStats.partitionUpdateRows.size() == table.getPartitions().size()) {
                return;
            }
            dropped = columnStats.partitionUpdateRows.keySet().stream()
                    .filter(id -> table.getPartition(id) == null).toList();
        } finally {
            table.readUnlock();
        }
        Set<Long> expiredPartition = Sets.newHashSet();
        String columnCondition = "AND col_id = " + StatisticsUtil.quote(col.getName());
        for (long partId : dropped) {
            columnStats.partitionUpdateRows.remove(partId);
            tableStats.partitionUpdateRows.remove(partId);
            // Skip the write when the task is cancelled: the job may already be in a
            // terminal state and have cleared the shared map, so no write should touch
            // it afterwards (a remove is a no-op on the cleared map, but keep the
            // invariant that cancelled tasks never write to the shared map).
            if (!killed) {
                jobInfo.partitionUpdateRows.remove(partId);
            }
            expiredPartition.add(partId);
            if (expiredPartition.size() == Config.max_allowed_in_element_num_of_delete) {
                String partitionCondition = " AND part_id in (" + Joiner.on(", ").join(expiredPartition) + ")";
                StatisticsRepository.dropPartitionsColumnStatistics(info.catalogId, info.dbId, info.tblId,
                        columnCondition, partitionCondition);
                expiredPartition.clear();
            }
        }
        if (expiredPartition.size() > 0) {
            String partitionCondition = " AND part_id in (" + Joiner.on(", ").join(expiredPartition) + ")";
            StatisticsRepository.dropPartitionsColumnStatistics(info.catalogId, info.dbId, info.tblId,
                    columnCondition, partitionCondition);
        }
    }

    @Override
    protected String getPartitionInfo(String partitionName) {
        return "partition " + SqlUtils.getIdentSql(partitionName);
    }

    @Override
    protected String configureLengthGuard(Map<String, String> params) {
        String guard = "__lc".equalsIgnoreCase(info.colName) ? "`__lc_guard`" : "`__lc`";
        // Consume the assertion even when LIMIT 0 eliminates the hot-value CTE.
        params.put("sampleFrequency", "SUM(IF(t0." + guard + ", 1, 0))");
        params.put("lengthFilter", " WHERE " + guard);
        return guard;
    }

    @Override
    protected Map<String, String> buildSqlParams() {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", FeConstants.INTERNAL_DB_NAME);
        params.put("columnStatTbl", StatisticConstants.TABLE_STATISTIC_TBL_NAME);
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", StatisticsUtil.escapeSQL(String.valueOf(info.colName)));
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        params.put("catalogName", SqlUtils.getIdentSql(catalog.getName()));
        params.put("dbName", SqlUtils.getIdentSql(db.getFullName()));
        params.put("colName", SqlUtils.getIdentSql(String.valueOf(info.colName)));
        params.put("tblName", SqlUtils.getIdentSql(String.valueOf(tbl.getName())));
        params.put("index", getIndex());
        params.put("preAggHint", "");
        addLengthAssertParam(params);
        return params;
    }

    protected String getIndex() {
        if (info.indexId == -1) {
            return "";
        } else {
            OlapTable olapTable = (OlapTable) this.tbl;
            olapTable.readLock();
            try {
                String name = olapTable.getIndexNameById(info.indexId);
                if (name == null) {
                    throw new AnalyzeSkipException("Statistics target index was removed: " + info.indexId);
                }
                return "index " + SqlUtils.getIdentSql(name);
            } finally {
                olapTable.readUnlock();
            }
        }
    }

    /**
     * Calculate rows to sample based on user given sample value.
     * @return Rows to sample.
     */
    protected long getSampleRows() {
        long sampleRows;
        if (tableSample.isPercent()) {
            tbl.readLock();
            try {
                sampleRows = (long) Math.max(tbl.getRowCount() * (tableSample.getSampleValue() / 100.0), 1);
            } finally {
                tbl.readUnlock();
            }
        } else {
            sampleRows = Math.max(tableSample.getSampleValue(), 1);
        }
        return sampleRows;
    }

    protected String concatColumnStatsId() {
        return info.tblId + "-" + info.indexId + "-" + info.colName;
    }

}

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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.sampling.ColumnPhysicalLayout;
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.RandomAggTabletSamplingMethod;
import org.apache.doris.statistics.sampling.SampledScanSqlRenderer;
import org.apache.doris.statistics.sampling.SamplingBudget;
import org.apache.doris.statistics.sampling.TabletSampleSpec;
import org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FullScanClause;
import org.apache.doris.statistics.sampling.TabletSampleSpec.KeyHashScanClause;
import org.apache.doris.statistics.sampling.TabletSamplingMethod;
import org.apache.doris.statistics.sampling.TabletSamplingRequest;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Each task analyze one column.
 */
public class HistogramTask extends BaseAnalysisTask {

    private static final String ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE = "INSERT INTO "
            + "${internalDB}.${histogramStatTbl} "
            + "SELECT "
            + "    CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}') AS id, "
            + "    ${catalogId} AS catalog_id, "
            + "    ${dbId} AS db_id, "
            + "    ${tblId} AS tbl_id, "
            + "    ${idxId} AS idx_id, "
            + "    '${colId}' AS col_id, "
            + "    ${sampleRate} AS sample_rate, "
            + "    HISTOGRAM(${colName}, ${maxBucketNum}) AS buckets, "
            + "    NOW() AS create_time "
            + "FROM "
            + "    ${tableRef}${index}${sampleHints}${sampleWhere}";

    public HistogramTask(AnalysisInfo info) {
        super(info);
    }

    @Override
    public void doExecute() throws Exception {
        if (killed) {
            return;
        }
        String sql = buildHistogramSql();
        StatisticsUtil.execUpdate(sql);
        Env.getCurrentEnv().getStatisticsCache().refreshHistogramSync(
                tbl.getDatabase().getCatalog().getId(), tbl.getDatabase().getId(), tbl.getId(), -1, col.getName());
        if (job != null) {
            job.taskDoneWithoutData(this);
        }
    }

    String buildHistogramSql() {
        Map<String, String> params = new HashMap<>();
        params.put("internalDB", SqlUtils.getIdentSql(FeConstants.INTERNAL_DB_NAME));
        params.put("histogramStatTbl", SqlUtils.getIdentSql(StatisticConstants.HISTOGRAM_TBL_NAME));
        params.put("catalogId", String.valueOf(catalog.getId()));
        params.put("dbId", String.valueOf(db.getId()));
        params.put("tblId", String.valueOf(tbl.getId()));
        params.put("idxId", String.valueOf(info.indexId));
        params.put("colId", StatisticsUtil.escapeSQL(String.valueOf(info.colName)));
        params.put("tableRef", tableRef());
        params.put("index", indexClause());
        params.put("colName", SqlUtils.getIdentSql(String.valueOf(info.colName)));
        params.put("sampleRate", getSampleRateFunction());
        params.put("sampleHints", "");
        params.put("sampleWhere", "");
        params.put("maxBucketNum", String.valueOf(info.maxBucketNum));
        if (info.analysisMethod == AnalysisMethod.SAMPLE && tbl instanceof OlapTable olapTable
                && StatisticsUtil.enableFixedBlockStatsSampling()) {
            applyBlockSample(olapTable, params);
        }
        return SampledScanSqlRenderer.checked(
                new StringSubstitutor(params).replace(ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE),
                OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES);
    }

    private void applyBlockSample(OlapTable table, Map<String, String> params) {
        long indexId = info.indexId == -1 ? table.getBaseIndexId() : info.indexId;
        long rows = job != null ? job.reportedIndexRows(table, indexId)
                : IndexSamplingSnapshot.reportedIndexRows(table, indexId, false);
        if (rows <= 0) {
            if (IndexSamplingSnapshot.hasWrittenPartition(table)) {
                throw new AnalyzeSkipException(
                        "Written partitions have no positive index row count; deferring histogram");
            }
            params.put("sampleRate", "1.0");
            return;
        }
        KeysType keys;
        boolean mow;
        boolean keyRangeSupported;
        boolean randomAggSupported;
        ColumnPhysicalLayout layout;
        List<String> keyColumnNames;
        table.readLock();
        try {
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
            Column physical = meta == null ? null : OlapAnalysisTask.resolveColumn(meta, info.colName);
            if (physical == null) {
                throw new AnalyzeSkipException("Histogram target column or index was removed: " + info.colName);
            }
            keys = meta.getKeysType();
            mow = table.getEnableUniqueKeyMergeOnWrite();
            keyRangeSupported = FixedBlockSampleSpec.supportsKeyRange(table, indexId);
            randomAggSupported = FixedBlockSampleSpec.supportsRandomAgg(table, indexId);
            layout = OlapAnalysisTask.columnLayout(table, meta, physical);
            keyColumnNames = keys == KeysType.AGG_KEYS
                    ? meta.getSchema().stream().filter(Column::isKey).map(Column::getName).toList()
                    : List.of();
        } finally {
            table.readUnlock();
        }
        IndexSamplingSnapshot frame = job != null ? job.samplingFrame(table, indexId)
                : IndexSamplingSnapshot.read(table, indexId);
        TabletSamplingMethod method = null;
        if ((keys == KeysType.DUP_KEYS || keys == KeysType.UNIQUE_KEYS && mow)
                && frame.allCandidatesSupportFixedBlock()) {
            method = new HierarchicalFixedBudgetTabletSamplingMethod();
        } else if ((keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS && !mow)
                && keyRangeSupported && frame.allCandidatesSupport(true)) {
            method = new HierarchicalFixedBudgetTabletSamplingMethod(Mode.KEY_RANGE_BLOCK);
        } else if (keys == KeysType.AGG_KEYS && randomAggSupported) {
            method = new RandomAggTabletSamplingMethod(frame.allCandidatesSupportFixedBlock(),
                    frame.allCandidatesSupport(true) && frame.allCandidatesSupportSharedKeyRange());
        }
        if (method == null) {
            if (!layout.key() && (keys == KeysType.AGG_KEYS || keys == KeysType.UNIQUE_KEYS && !mow)) {
                throw new AnalyzeSkipException(
                        "Sample histogram cannot collect unmerged value column: " + info.colName);
            }
            return;
        }
        params.put("sampleRate", "0.00000000000000001");
        int whereReserve = method instanceof RandomAggTabletSamplingMethod
                ? SampledScanSqlRenderer.renderKeyHashWhere(
                        new KeyHashScanClause(Optional.empty(), Long.MAX_VALUE, 1), keyColumnNames)
                        .getBytes(StandardCharsets.UTF_8).length
                : 0;
        int baseBytes = new StringSubstitutor(params).replace(ANALYZE_HISTOGRAM_SQL_TEMPLATE_TABLE)
                .getBytes(StandardCharsets.UTF_8).length + 1 + whereReserve;
        long targetRows = info.samplePercent > 0
                ? Math.max(1L, (long) (rows * (info.samplePercent / 100.0)))
                : Math.max(1L, info.sampleRows > 0 ? info.sampleRows : StatisticsUtil.getHugeTableSampleRows());
        long seed = job != null ? job.samplingSeed(tableSample == null ? -1 : tableSample.getSeek())
                : (ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE);
        SamplingBudget budget = new SamplingBudget(targetRows,
                StatisticsUtil.getStatisticsSampleUnitRows(),
                StatisticsUtil.getStatisticsMaxSampleTablets(), 4,
                OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES - baseBytes);
        TabletSampleSpec spec = method.build(new TabletSamplingRequest(frame.scope(), layout, budget, rows, seed));
        if (spec.scanClause() instanceof FullScanClause) {
            params.put("sampleRate", "1.0");
            return;
        }
        String rendered = SampledScanSqlRenderer.render(spec.scanClause(), OlapAnalysisTask.MAX_INTERNAL_SQL_BYTES);
        params.put("sampleHints", rendered.isEmpty() ? "" : " " + rendered);
        params.put("sampleWhere", SampledScanSqlRenderer.renderKeyHashWhere(spec.scanClause(), keyColumnNames));
        params.put("sampleRate", Double.toString(((EqualProbabilitySamplingSemantics) spec.semantics()).probability()));
    }

    private String tableRef() {
        String prefix = catalog == null || catalog.getName() == null ? ""
                : SqlUtils.getIdentSql(catalog.getName()) + ".";
        return prefix + SqlUtils.getIdentSql(db.getFullName()) + "." + SqlUtils.getIdentSql(tbl.getName());
    }

    private String indexClause() {
        if (info.indexId == -1 || !(tbl instanceof OlapTable olapTable)) {
            return "";
        }
        olapTable.readLock();
        try {
            String name = olapTable.getIndexNameById(info.indexId);
            if (name == null) {
                throw new AnalyzeSkipException("Histogram target index was removed: " + info.indexId);
            }
            return " INDEX " + SqlUtils.getIdentSql(name);
        } finally {
            olapTable.readUnlock();
        }
    }

    @Override
    protected void doSample() {
    }

    @Override
    protected void deleteNotExistPartitionStats(AnalysisInfo jobInfo) throws DdlException {
    }

    private String getSampleRateFunction() {
        if (info.analysisMethod == AnalysisMethod.FULL) {
            return "0";
        }
        if (info.samplePercent > 0) {
            return String.valueOf(info.samplePercent / 100.0);
        } else {
            long rowCount = tbl.getRowCount() > 0 ? tbl.getRowCount() : 1;
            double sampRate = (double) info.sampleRows / rowCount;
            return sampRate >= 1 ? "1.0" : String.format("%.4f", sampRate);
        }
    }
}

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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.analysis.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TableStatsMeta implements Writable, GsonPostProcessable {

    @SerializedName("ctlId")
    public final long ctlId;

    @SerializedName("ctln")
    public final String ctlName;

    @SerializedName("dbId")
    public final long dbId;

    @SerializedName("dbn")
    public final String dbName;

    @SerializedName("tblId")
    public final long tblId;

    @SerializedName("tbln")
    public final String tblName;

    @SerializedName("idxId")
    public final long idxId;
    @SerializedName("updatedRows")
    public final AtomicLong updatedRows = new AtomicLong();

    // We would like to analyze tables which queried frequently with higher priority in the future.
    @SerializedName("queriedTimes")
    public final AtomicLong queriedTimes = new AtomicLong();

    // Used for external table.
    @SerializedName("rowCount")
    public long rowCount;

    @SerializedName("updateTime")
    public long updatedTime;

    @SerializedName("lat")
    public long lastAnalyzeTime;

    @SerializedName("colNameToColStatsMeta")
    private ConcurrentMap<String, ColStatsMeta> deprecatedColNameToColStatsMeta = new ConcurrentHashMap<>();

    @SerializedName("colToColStatsMeta")
    // <IndexName, ColumnName> -> ColStatsMeta
    private ConcurrentMap<Pair<String, String>, ColStatsMeta> colToColStatsMeta = new ConcurrentHashMap<>();

    @SerializedName("trigger")
    public JobType jobType;

    @SerializedName("newPartitionLoaded")
    public AtomicBoolean partitionChanged = new AtomicBoolean(false);

    @SerializedName("userInjected")
    public boolean userInjected;

    @SerializedName("pur")
    public ConcurrentMap<Long, Long> partitionUpdateRows = new ConcurrentHashMap<>();

    @SerializedName("irc")
    private ConcurrentMap<Long, Long> indexesRowCount = new ConcurrentHashMap<>();

    // The value of updatedRows when indexesRowCount was collected, i.e. the number of rows the collected
    // row count already includes. The rows loaded after that point are the delta row count of the table.
    // It is kept here, and not derived from colToColStatsMeta, so that dropping the column statistics of
    // the table doesn't lose it. -1 means no row count has ever been collected from the table.
    @SerializedName("updatedRowsBase")
    private final AtomicLong updatedRowsBase = new AtomicLong(-1);

    @VisibleForTesting
    public TableStatsMeta() {
        ctlId = 0;
        ctlName = null;
        dbId = 0;
        dbName = null;
        tblId = 0;
        tblName = null;
        idxId = 0;
    }

    // It's necessary to store these fields separately from AnalysisInfo, since the lifecycle between AnalysisInfo
    // and TableStats is quite different.
    public TableStatsMeta(long rowCount, AnalysisInfo analyzedJob, TableIf table) {
        this.ctlId = table.getDatabase().getCatalog().getId();
        this.ctlName = table.getDatabase().getCatalog().getName();
        this.dbId = table.getDatabase().getId();
        this.dbName = table.getDatabase().getFullName();
        this.tblId = table.getId();
        this.tblName = table.getName();
        this.idxId = -1;
        this.rowCount = rowCount;
        update(analyzedJob, table);
    }

    /**
     * Create a record for a table which doesn't have one yet, in the state of an empty table. The rows
     * loaded into the table are accumulated by {@link AnalysisManager#replayUpdateRowsRecord}, so a record
     * has to exist before the first load, otherwise these rows can never be turned into a row count.
     */
    public TableStatsMeta(OlapTable table) {
        this.ctlId = table.getDatabase().getCatalog().getId();
        this.ctlName = table.getDatabase().getCatalog().getName();
        this.dbId = table.getDatabase().getId();
        this.dbName = table.getDatabase().getFullName();
        this.tblId = table.getId();
        this.tblName = table.getName();
        this.idxId = -1;
        this.indexesRowCount = buildEmptyIndexRowCount(table);
        this.updatedRowsBase.set(0);
    }

    /**
     * TRUNCATE TABLE removes all the data of the table. Reset this record back to the state of an empty
     * table instead of dropping it, so that the rows loaded after the truncation can still be accumulated
     * into {@link #updatedRows} and be reported as the row count of the table.
     * <p>
     * The transition runs under the monitor of this record, which {@link #getRowCountWithDeltaRows} also
     * takes, so a planner which reads the row count of the table without holding its lock either sees the
     * whole transition or none of it. The order the fields are published in matters for the readers which
     * don't take the monitor, for instance SHOW TABLE STATS: the baseline first makes the delta empty while
     * the collected row count is still the one of the removed data, so the emptied row count is only
     * published once no row of the removed data is counted as a delta row anymore.
     */
    public synchronized void reset(OlapTable table) {
        updatedRowsBase.set(updatedRows.get());
        indexesRowCount = buildEmptyIndexRowCount(table);
        updatedRows.set(0);
        // None of the rows loaded from now on is included in the collected row count. They are all delta rows.
        updatedRowsBase.set(0);
        rowCount = 0;
        partitionUpdateRows.clear();
        // Drop the column statistics baseline: the row count captured by the previous analysis described
        // the removed data, it must not cancel out the rows loaded after the truncation.
        colToColStatsMeta.clear();
        // The statistics of the removed data is stale, let the analyzer collect it again.
        partitionChanged.set(true);
        // The injected statistics described the removed data, it no longer applies to this table.
        userInjected = false;
        // The emptied table has never been analyzed, and no analyze job describes it any more.
        updatedTime = 0;
        lastAnalyzeTime = 0;
        jobType = null;
    }

    private static ConcurrentMap<Long, Long> buildEmptyIndexRowCount(OlapTable table) {
        // TRUNCATE TABLE removed the data of every index of the table, so every index whose row count is known
        // to follow the row count of the base index is known to be empty. The row count of an index which
        // aggregates is unknown until the backends report it, so it is not claimed to be 0 here.
        ConcurrentMap<Long, Long> indexRowCount = new ConcurrentHashMap<>();
        for (Long indexId : table.getIndexIdList()) {
            if (keepsOneRowPerBaseRow(table, indexId)) {
                indexRowCount.put(indexId, 0L);
            }
        }
        return indexRowCount;
    }

    /**
     * Whether the rows loaded into the base index are the rows of this index as well. That holds for the base
     * index itself and for an index which keeps one row per base row, i.e. a duplicate key index whose columns
     * are all plain. An index which aggregates, or which merges the rows of a unique key table, has a smaller
     * row count of its own, so the rows loaded into the base index must not be added to it.
     */
    public static boolean keepsOneRowPerBaseRow(OlapTable table, long indexId) {
        if (indexId == table.getBaseIndexId()) {
            return true;
        }
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(indexId);
        if (indexMeta == null || indexMeta.getKeysType() != KeysType.DUP_KEYS) {
            return false;
        }
        for (Column column : indexMeta.getSchema()) {
            // A key column has no aggregation type at all, the value columns of a duplicate key index are
            // NONE. Neither of them merges rows, only an aggregating column does.
            AggregateType aggregationType = column.getAggregationType();
            if (aggregationType != null && aggregationType != AggregateType.NONE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TableStatsMeta read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        TableStatsMeta tableStats = GsonUtils.GSON.fromJson(json, TableStatsMeta.class);
        // Might be null counterintuitively, for compatible
        if (tableStats.colToColStatsMeta == null) {
            tableStats.colToColStatsMeta = new ConcurrentHashMap<>();
        }
        if (tableStats.deprecatedColNameToColStatsMeta != null) {
            tableStats.convertDeprecatedColStatsToNewVersion();
        }
        return tableStats;
    }

    public ColStatsMeta findColumnStatsMeta(String indexName, String colName) {
        return colToColStatsMeta.get(Pair.of(indexName, colName));
    }

    public void removeColumn(String indexName, String colName) {
        colToColStatsMeta.remove(Pair.of(indexName, colName));
    }

    public Set<Pair<String, String>> analyzeColumns() {
        return colToColStatsMeta.keySet();
    }

    /**
     * Apply the outcome of an analyze job to this record. Like {@link #reset}, the transition runs under the
     * monitor of this record so that a planner reading the row count of the table without holding its lock
     * doesn't pair the collected row count of this analysis with the baseline of another one.
     */
    public synchronized void update(AnalysisInfo analyzedJob, TableIf tableIf) {
        updatedTime = analyzedJob.tblUpdateTime;
        lastAnalyzeTime = analyzedJob.createTime;
        if (analyzedJob.userInject) {
            userInjected = true;
        }
        for (Pair<String, String> colPair : analyzedJob.jobColumns) {
            ColStatsMeta colStatsMeta = colToColStatsMeta.get(colPair);
            if (colStatsMeta == null) {
                colToColStatsMeta.put(colPair, new ColStatsMeta(analyzedJob.createTime, analyzedJob.analysisMethod,
                        analyzedJob.analysisType, analyzedJob.jobType, 0, analyzedJob.rowCount,
                        analyzedJob.updateRows, analyzedJob.tableVersion,
                        analyzedJob.enablePartition ? analyzedJob.partitionUpdateRows : null));
            } else {
                colStatsMeta.updatedTime = analyzedJob.createTime;
                colStatsMeta.analysisType = analyzedJob.analysisType;
                colStatsMeta.analysisMethod = analyzedJob.analysisMethod;
                colStatsMeta.jobType = analyzedJob.jobType;
                colStatsMeta.updatedRows = analyzedJob.updateRows;
                colStatsMeta.rowCount = analyzedJob.rowCount;
                colStatsMeta.tableVersion = analyzedJob.tableVersion;
                if (analyzedJob.enablePartition) {
                    if (colStatsMeta.partitionUpdateRows == null) {
                        colStatsMeta.partitionUpdateRows = new ConcurrentHashMap<>();
                    }
                    colStatsMeta.partitionUpdateRows.putAll(analyzedJob.partitionUpdateRows);
                }
            }
        }
        jobType = analyzedJob.jobType;
        if (tableIf != null) {
            if (tableIf instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) tableIf;
                // The collected row counts published below already include the rows which had been loaded when
                // the job was built, remember how many they were, they are not delta rows. The baseline may
                // only advance together with the collected base index row count, an analysis of another index
                // (a materialized view) doesn't touch it.
                // NOTICE: the baseline is published before the collected counts. They are both read without
                // the table lock by the planner, and a reader which pairs the collected count of this analysis
                // with the baseline of the previous one counts the rows loaded since that previous analysis
                // twice.
                if (analyzedJob.indexesRowCount.containsKey(olapTable.getBaseIndexId())) {
                    // A row count supplied by the user replaces the collected one and describes the table as
                    // of now, so the rows loaded so far are part of it and the baseline has to move to the
                    // current value. Keeping the older baseline would count those rows again once DROP STATS
                    // clears userInjected.
                    updatedRowsBase.set(analyzedJob.userInject ? updatedRows.get() : analyzedJob.updateRows);
                }
                indexesRowCount.putAll(analyzedJob.indexesRowCount);
                clearStaleIndexRowCount(olapTable);
                if (analyzedJob.jobColumns.containsAll(
                        olapTable.getColumnIndexPairs(olapTable.getSchemaAllIndexes(false)
                                        .stream()
                                        .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                                        .map(Column::getName).collect(Collectors.toSet()))
                                .stream()
                                .filter(c -> StatisticsUtil.canCollectColumn(olapTable.getIndexMetaByIndexId(
                                        olapTable.getIndexIdByName(c.first)).getColumnByName(c.second),
                                        olapTable, true, olapTable.getIndexIdByName(c.first)))
                                .collect(Collectors.toSet()))) {
                    partitionChanged.set(false);
                }
            }
            rowCount = analyzedJob.rowCount;
            // Set userInject back to false after manual analyze.
            if (JobType.MANUAL.equals(jobType) && !analyzedJob.userInject) {
                userInjected = false;
            }
        }
    }

    public void convertDeprecatedColStatsToNewVersion() {
        deprecatedColNameToColStatsMeta = null;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (partitionUpdateRows == null) {
            partitionUpdateRows = new ConcurrentHashMap<>();
        }
        if (indexesRowCount == null) {
            indexesRowCount = new ConcurrentHashMap<>();
        }
        if (colToColStatsMeta == null) {
            colToColStatsMeta = new ConcurrentHashMap<>();
        }
    }

    public long getRowCount(long indexId) {
        return indexesRowCount.getOrDefault(indexId, -1L);
    }

    protected void clearStaleIndexRowCount(OlapTable table) {
        Iterator<Long> iterator = indexesRowCount.keySet().iterator();
        List<Long> indexIds = table.getIndexIdList();
        while (iterator.hasNext()) {
            long key = iterator.next();
            if (!indexIds.contains(key)) {
                iterator.remove();
            }
        }
    }

    // For unit test only.
    protected void addIndexRowForTest(long indexId, long rowCount) {
        indexesRowCount.put(indexId, rowCount);
    }

    // For unit test only. Simulate a record written before updatedRowsBase was recorded in the table stats.
    protected void clearUpdatedRowsBaseForTest() {
        updatedRowsBase.set(-1);
    }

    /**
     * The delta rows is the rows loaded since the row count of this table was collected, i.e. the rows
     * which the collected row count doesn't include yet. It is 0 for a table with statistics supplied by
     * the user, those are reported as the row count of the table directly.
     */
    public long getBaseIndexDeltaRowCount(OlapTable table) {
        if (userInjected) {
            return 0;
        }
        long collectedRowCountBase = updatedRowsBase.get();
        if (collectedRowCountBase >= 0) {
            return updatedRows.get() - collectedRowCountBase;
        }
        // A record written before updatedRowsBase existed has no baseline of its own. Derive it from the
        // collected column statistics, which is where it used to live. Once they are all dropped the
        // baseline is unknown, so no row is reported as a delta row.
        if (colToColStatsMeta == null || colToColStatsMeta.isEmpty()) {
            return 0;
        }
        long maxUpdateRows = 0;
        String baseIndexName = table.getIndexNameById(table.getBaseIndexId());
        for (Map.Entry<Pair<String, String>, ColStatsMeta> entry : colToColStatsMeta.entrySet()) {
            if (entry.getKey().first.equals(baseIndexName) && entry.getValue().updatedRows > maxUpdateRows) {
                maxUpdateRows = entry.getValue().updatedRows;
            }
        }
        return updatedRows.get() - maxUpdateRows;
    }

    /**
     * The row count of the index together with the rows loaded since it was collected, i.e. the row count of
     * the table. Both are read by the planner without the table lock, for instance while it plans a direct
     * scan of a materialized view, so they have to come from the same state of this record: a collected row
     * count paired with the baseline of another analysis, or of a truncation, would count rows twice or miss
     * them. The rows loaded while this call runs are not part of the snapshot, whichever state reads them
     * accumulates them in {@link #updatedRows} and reports them as delta rows.
     */
    public synchronized long getRowCountWithDeltaRows(OlapTable table, long indexId) {
        long rowCount = getRowCount(indexId);
        if (!keepsOneRowPerBaseRow(table, indexId)) {
            // The index aggregates, or it merges the rows of a unique key table: it has its own, smaller row
            // count, and the rows loaded into the base index would overstate it.
            return rowCount;
        }
        return rowCount + getBaseIndexDeltaRowCount(table);
    }

    public boolean isColumnsStatsEmpty() {
        return colToColStatsMeta == null || colToColStatsMeta.isEmpty();
    }
}

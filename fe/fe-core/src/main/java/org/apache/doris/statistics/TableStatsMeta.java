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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.JobType;
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
    public ConcurrentMap<Long, Long> indexesRowCount = new ConcurrentHashMap<>();

    @SerializedName("ircut")
    public ConcurrentMap<Long, Long> indexesRowCountUpdateTime = new ConcurrentHashMap<>();

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

    public void removeAllColumn() {
        colToColStatsMeta.clear();
    }

    public Set<Pair<String, String>> analyzeColumns() {
        return colToColStatsMeta.keySet();
    }

    public void update(AnalysisInfo analyzedJob, TableIf tableIf) {
        updatedTime = analyzedJob.tblUpdateTime;
        userInjected = analyzedJob.userInject;
        for (Pair<String, String> colPair : analyzedJob.jobColumns) {
            ColStatsMeta colStatsMeta = colToColStatsMeta.get(colPair);
            if (colStatsMeta == null) {
                colToColStatsMeta.put(colPair, new ColStatsMeta(analyzedJob.createTime, analyzedJob.analysisMethod,
                        analyzedJob.analysisType, analyzedJob.jobType, 0, analyzedJob.rowCount,
                        analyzedJob.updateRows, analyzedJob.enablePartition ? analyzedJob.partitionUpdateRows : null));
            } else {
                colStatsMeta.updatedTime = analyzedJob.tblUpdateTime;
                colStatsMeta.analysisType = analyzedJob.analysisType;
                colStatsMeta.analysisMethod = analyzedJob.analysisMethod;
                colStatsMeta.jobType = analyzedJob.jobType;
                colStatsMeta.updatedRows = analyzedJob.updateRows;
                colStatsMeta.rowCount = analyzedJob.rowCount;
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
                indexesRowCount.putAll(analyzedJob.indexesRowCount);
                indexesRowCountUpdateTime.putAll(analyzedJob.indexesRowCountUpdateTime);
                clearStaleIndexRowCountAndTime((OlapTable) tableIf);
            }
            rowCount = analyzedJob.rowCount;
            if (rowCount == 0 && AnalysisMethod.SAMPLE.equals(analyzedJob.analysisMethod)) {
                return;
            }
            if (analyzedJob.jobColumns.containsAll(
                    tableIf.getColumnIndexPairs(
                    tableIf.getSchemaAllIndexes(false).stream()
                            .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                            .map(Column::getName).collect(Collectors.toSet())))) {
                partitionChanged.set(false);
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
        if (indexesRowCountUpdateTime == null) {
            indexesRowCountUpdateTime = new ConcurrentHashMap<>();
        }
    }

    public long getRowCount(long indexId) {
        return indexesRowCount.getOrDefault(indexId, -1L);
    }

    public long getRowCountUpdateTime(long indexId) {
        return indexesRowCountUpdateTime.getOrDefault(indexId, 0L);
    }

    private void clearStaleIndexRowCountAndTime(OlapTable table) {
        Iterator<Long> iterator = indexesRowCount.keySet().iterator();
        List<Long> indexIds = table.getIndexIds();
        while (iterator.hasNext()) {
            long key = iterator.next();
            if (indexIds.contains(key)) {
                iterator.remove();
            }
        }
        iterator = indexesRowCountUpdateTime.keySet().iterator();
        while (iterator.hasNext()) {
            long key = iterator.next();
            if (indexIds.contains(key)) {
                iterator.remove();
            }
        }
    }

    public long getBaseIndexDeltaRowCount(OlapTable table) {
        if (colToColStatsMeta == null) {
            return -1;
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
}

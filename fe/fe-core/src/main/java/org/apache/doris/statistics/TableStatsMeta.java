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
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisInfo.JobType;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TableStatsMeta implements Writable {

    @SerializedName("tblId")
    public final long tblId;

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
    public AtomicBoolean newPartitionLoaded = new AtomicBoolean(false);

    @SerializedName("userInjected")
    public boolean userInjected;

    @VisibleForTesting
    public TableStatsMeta() {
        tblId = 0;
        idxId = 0;
    }

    // It's necessary to store these fields separately from AnalysisInfo, since the lifecycle between AnalysisInfo
    // and TableStats is quite different.
    public TableStatsMeta(long rowCount, AnalysisInfo analyzedJob, TableIf table) {
        this.tblId = table.getId();
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

    public Set<Pair<String, String>> analyzeColumns() {
        return colToColStatsMeta.keySet();
    }

    public void update(AnalysisInfo analyzedJob, TableIf tableIf) {
        updatedTime = analyzedJob.tblUpdateTime;
        userInjected = analyzedJob.userInject;
        for (Pair<String, String> colPair : analyzedJob.jobColumns) {
            ColStatsMeta colStatsMeta = colToColStatsMeta.get(colPair);
            if (colStatsMeta == null) {
                colToColStatsMeta.put(colPair, new ColStatsMeta(updatedTime,
                        analyzedJob.analysisMethod, analyzedJob.analysisType, analyzedJob.jobType, 0));
            } else {
                colStatsMeta.updatedTime = updatedTime;
                colStatsMeta.analysisType = analyzedJob.analysisType;
                colStatsMeta.analysisMethod = analyzedJob.analysisMethod;
                colStatsMeta.jobType = analyzedJob.jobType;
            }
        }
        jobType = analyzedJob.jobType;
        if (tableIf != null) {
            if (tableIf instanceof OlapTable) {
                rowCount = analyzedJob.emptyJob ? 0 : tableIf.getRowCount();
            }
            if (analyzedJob.emptyJob) {
                return;
            }
            if (analyzedJob.jobColumns.containsAll(
                    tableIf.getColumnIndexPairs(
                    tableIf.getSchemaAllIndexes(false).stream().map(Column::getName).collect(Collectors.toSet())))) {
                updatedRows.set(0);
                newPartitionLoaded.set(false);
            }
            if (tableIf instanceof OlapTable) {
                PartitionInfo partitionInfo = ((OlapTable) tableIf).getPartitionInfo();
                if (partitionInfo != null && analyzedJob.jobColumns
                        .containsAll(tableIf.getColumnIndexPairs(partitionInfo.getPartitionColumns().stream()
                            .map(Column::getName).collect(Collectors.toSet())))) {
                    newPartitionLoaded.set(false);
                }
            }
        }
    }

    public void convertDeprecatedColStatsToNewVersion() {
        deprecatedColNameToColStatsMeta = null;
    }
}

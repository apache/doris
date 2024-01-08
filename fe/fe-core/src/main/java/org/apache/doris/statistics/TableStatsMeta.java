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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
    private ConcurrentMap<String, ColStatsMeta> colNameToColStatsMeta = new ConcurrentHashMap<>();

    @SerializedName("trigger")
    public JobType jobType;

    @SerializedName("newPartitionLoaded")
    public AtomicBoolean newPartitionLoaded = new AtomicBoolean(false);

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
        if (tableStats.colNameToColStatsMeta == null) {
            tableStats.colNameToColStatsMeta = new ConcurrentHashMap<>();
        }
        return tableStats;
    }

    public long findColumnLastUpdateTime(String colName) {
        ColStatsMeta colStatsMeta = colNameToColStatsMeta.get(colName);
        if (colStatsMeta == null) {
            return 0;
        }
        return colStatsMeta.updatedTime;
    }

    public ColStatsMeta findColumnStatsMeta(String colName) {
        return colNameToColStatsMeta.get(colName);
    }

    public void removeColumn(String colName) {
        colNameToColStatsMeta.remove(colName);
    }

    public Set<String> analyzeColumns() {
        return colNameToColStatsMeta.keySet();
    }

    public void reset() {
        updatedTime = 0;
        colNameToColStatsMeta.values().forEach(ColStatsMeta::clear);
    }

    public void update(AnalysisInfo analyzedJob, TableIf tableIf) {
        updatedTime = analyzedJob.tblUpdateTime;
        String colNameStr = analyzedJob.colName;
        // colName field AnalyzeJob's format likes: "[col1, col2]", we need to remove brackets here
        // TODO: Refactor this later
        if (analyzedJob.colName.startsWith("[") && analyzedJob.colName.endsWith("]")) {
            colNameStr = colNameStr.substring(1, colNameStr.length() - 1);
        }
        List<String> cols = Arrays.stream(colNameStr.split(",")).map(String::trim).collect(Collectors.toList());
        for (String col : cols) {
            ColStatsMeta colStatsMeta = colNameToColStatsMeta.get(col);
            if (colStatsMeta == null) {
                colNameToColStatsMeta.put(col, new ColStatsMeta(updatedTime,
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
                rowCount = tableIf.getRowCount();
            }
            if (!analyzedJob.emptyJob && analyzedJob.colToPartitions.keySet()
                    .containsAll(tableIf.getBaseSchema().stream()
                            .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                            .map(Column::getName).collect(Collectors.toSet()))) {
                updatedRows.set(0);
                newPartitionLoaded.set(false);
            }
            if (tableIf instanceof OlapTable) {
                PartitionInfo partitionInfo = ((OlapTable) tableIf).getPartitionInfo();
                if (partitionInfo != null && analyzedJob.colToPartitions.keySet()
                        .containsAll(partitionInfo.getPartitionColumns().stream()
                            .map(Column::getName).collect(Collectors.toSet()))) {
                    newPartitionLoaded.set(false);
                }
            }
        }
    }
}

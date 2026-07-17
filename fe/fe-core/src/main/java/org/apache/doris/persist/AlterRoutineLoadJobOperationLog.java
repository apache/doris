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

package org.apache.doris.persist;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterRoutineLoadJobOperationLog  implements Writable {

    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "jobProperties")
    private Map<String, String> jobProperties;
    @SerializedName(value = "dataSourceProperties")
    private AbstractDataSourceProperties dataSourceProperties;
    @SerializedName(value = "targetTableId")
    private long targetTableId;
    @SerializedName(value = "routineLoadDesc")
    private RoutineLoadDescSnapshot routineLoadDesc;

    public AlterRoutineLoadJobOperationLog(long jobId, Map<String, String> jobProperties,
            AbstractDataSourceProperties dataSourceProperties) {
        this(jobId, jobProperties, dataSourceProperties, 0L);
    }

    public AlterRoutineLoadJobOperationLog(long jobId, Map<String, String> jobProperties,
            AbstractDataSourceProperties dataSourceProperties, long targetTableId) {
        this(jobId, jobProperties, dataSourceProperties, targetTableId, null);
    }

    public AlterRoutineLoadJobOperationLog(long jobId, Map<String, String> jobProperties,
            AbstractDataSourceProperties dataSourceProperties, long targetTableId,
            RoutineLoadDesc routineLoadDesc) {
        this.jobId = jobId;
        this.jobProperties = jobProperties;
        this.dataSourceProperties = dataSourceProperties;
        this.targetTableId = targetTableId;
        this.routineLoadDesc = routineLoadDesc == null ? null : new RoutineLoadDescSnapshot(routineLoadDesc);
    }

    public long getJobId() {
        return jobId;
    }

    public Map<String, String> getJobProperties() {
        return jobProperties;
    }

    public AbstractDataSourceProperties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public long getTargetTableId() {
        return targetTableId;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        return routineLoadDesc == null ? null : routineLoadDesc.toRoutineLoadDesc();
    }

    private static class RoutineLoadDescSnapshot {
        @SerializedName("columnSeparator")
        private SeparatorSnapshot columnSeparator;
        @SerializedName("lineDelimiter")
        private SeparatorSnapshot lineDelimiter;
        @SerializedName("columnsInfo")
        private List<ImportColumnDesc> columnsInfo;
        @SerializedName("precedingFilter")
        private Expr precedingFilter;
        @SerializedName("filter")
        private Expr filter;
        @SerializedName("partitionNamesInfo")
        private PartitionNamesInfo partitionNamesInfo;
        @SerializedName("deleteCondition")
        private Expr deleteCondition;
        @SerializedName("mergeType")
        private LoadTask.MergeType mergeType;
        @SerializedName("sequenceColName")
        private String sequenceColName;

        private RoutineLoadDescSnapshot(RoutineLoadDesc routineLoadDesc) {
            this.columnSeparator = SeparatorSnapshot.fromSeparator(routineLoadDesc.getColumnSeparator());
            this.lineDelimiter = SeparatorSnapshot.fromSeparator(routineLoadDesc.getLineDelimiter());
            this.columnsInfo = routineLoadDesc.getColumnsInfo();
            this.precedingFilter = routineLoadDesc.getPrecedingFilter();
            this.filter = routineLoadDesc.getFilter();
            this.partitionNamesInfo = routineLoadDesc.getPartitionNamesInfo();
            this.deleteCondition = routineLoadDesc.getDeleteCondition();
            this.mergeType = routineLoadDesc.getMergeType();
            this.sequenceColName = routineLoadDesc.getSequenceColName();
        }

        private RoutineLoadDesc toRoutineLoadDesc() {
            return new RoutineLoadDesc(SeparatorSnapshot.toSeparator(columnSeparator),
                    SeparatorSnapshot.toSeparator(lineDelimiter), columnsInfo, precedingFilter, filter,
                    partitionNamesInfo, deleteCondition, mergeType, sequenceColName);
        }
    }

    private static class SeparatorSnapshot {
        @SerializedName("separator")
        private String separator;
        @SerializedName("oriSeparator")
        private String oriSeparator;

        private SeparatorSnapshot(Separator separator) {
            this.separator = separator.getSeparator();
            this.oriSeparator = separator.getOriSeparator();
        }

        private static SeparatorSnapshot fromSeparator(Separator separator) {
            return separator == null ? null : new SeparatorSnapshot(separator);
        }

        private static Separator toSeparator(SeparatorSnapshot snapshot) {
            return snapshot == null ? null : new Separator(snapshot.separator, snapshot.oriSeparator);
        }
    }

    public static AlterRoutineLoadJobOperationLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterRoutineLoadJobOperationLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}

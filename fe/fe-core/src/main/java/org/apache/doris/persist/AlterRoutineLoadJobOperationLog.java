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
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.AbstractDataSourceProperties;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.LoadTaskInfo.ImportColumnDescs;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AlterRoutineLoadJobOperationLog  implements Writable {

    @SerializedName(value = "jobId")
    private long jobId;
    @SerializedName(value = "jobProperties")
    private Map<String, String> jobProperties;
    @SerializedName(value = "dataSourceProperties")
    private AbstractDataSourceProperties dataSourceProperties;
    @SerializedName(value = "columnDescs")
    private ImportColumnDescs columnDescs;
    @SerializedName(value = "hrld")
    private boolean hasRoutineLoadDesc;
    @SerializedName(value = "cs")
    private String columnSeparator;
    @SerializedName(value = "ocs")
    private String oriColumnSeparator;
    @SerializedName(value = "ld")
    private String lineDelimiter;
    @SerializedName(value = "old")
    private String oriLineDelimiter;
    @SerializedName(value = "pf")
    private Expr precedingFilter;
    @SerializedName(value = "filter")
    private Expr filter;
    @SerializedName(value = "dc")
    private Expr deleteCondition;
    @SerializedName(value = "pni")
    private PartitionNamesInfo partitionNamesInfo;
    @SerializedName(value = "mt")
    private LoadTask.MergeType mergeType;
    @SerializedName(value = "mts")
    private boolean mergeTypeSpecified;
    @SerializedName(value = "scn")
    private String sequenceColName;

    public AlterRoutineLoadJobOperationLog(long jobId, Map<String, String> jobProperties,
            AbstractDataSourceProperties dataSourceProperties) {
        this(jobId, jobProperties, dataSourceProperties, null);
    }

    public AlterRoutineLoadJobOperationLog(long jobId, Map<String, String> jobProperties,
            AbstractDataSourceProperties dataSourceProperties, RoutineLoadDesc routineLoadDesc) {
        this.jobId = jobId;
        this.jobProperties = new HashMap<>(jobProperties);
        this.dataSourceProperties = dataSourceProperties;
        if (routineLoadDesc == null) {
            return;
        }
        hasRoutineLoadDesc = true;
        setSeparatorFields(routineLoadDesc.getColumnSeparator(), true);
        setSeparatorFields(routineLoadDesc.getLineDelimiter(), false);
        precedingFilter = routineLoadDesc.getPrecedingFilter();
        filter = routineLoadDesc.getFilter();
        deleteCondition = routineLoadDesc.getDeleteCondition();
        partitionNamesInfo = routineLoadDesc.getPartitionNamesInfo();
        mergeType = routineLoadDesc.getMergeType();
        mergeTypeSpecified = routineLoadDesc.isMergeTypeSpecified();
        sequenceColName = routineLoadDesc.getSequenceColName();
        if (routineLoadDesc.getColumnsInfo() != null) {
            this.columnDescs = new ImportColumnDescs();
            this.columnDescs.descs.addAll(routineLoadDesc.getColumnsInfo());
        }
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

    public ImportColumnDescs getColumnDescs() {
        return columnDescs;
    }

    public RoutineLoadDesc getRoutineLoadDesc() {
        if (!hasRoutineLoadDesc) {
            return null;
        }
        return new RoutineLoadDesc(
                buildSeparator(columnSeparator, oriColumnSeparator),
                buildSeparator(lineDelimiter, oriLineDelimiter),
                columnDescs == null ? null : new ArrayList<>(columnDescs.descs), precedingFilter,
                filter, partitionNamesInfo, deleteCondition, mergeType, mergeTypeSpecified, sequenceColName);
    }

    private void setSeparatorFields(Separator separator, boolean isColumnSeparator) {
        if (separator == null) {
            return;
        }
        if (isColumnSeparator) {
            columnSeparator = separator.getSeparator();
            oriColumnSeparator = separator.getOriSeparator();
        } else {
            lineDelimiter = separator.getSeparator();
            oriLineDelimiter = separator.getOriSeparator();
        }
    }

    private static Separator buildSeparator(String separator, String oriSeparator) {
        if (separator == null && oriSeparator == null) {
            return null;
        }
        return new Separator(separator, oriSeparator);
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

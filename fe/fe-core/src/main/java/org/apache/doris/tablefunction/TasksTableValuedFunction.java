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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TTasksMetadataParams;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * tasks("type" = "mv").
 */
public class TasksTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "tasks";
    private static final String TYPE = "type";

    private static final ImmutableSet<String> PROPERTIES_SET = ImmutableSet.of(TYPE);

    private final JobType jobType;

    public TasksTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase())) {
                throw new AnalysisException("'" + key + "' is invalid property");
            }
            validParams.put(key.toLowerCase(), params.get(key));
        }
        String type = validParams.get(TYPE);
        if (type == null) {
            throw new AnalysisException("Invalid task metadata query");
        }
        JobType jobType = JobType.valueOf(type.toUpperCase());
        if (jobType == null) {
            throw new AnalysisException("Invalid task metadata query");
        }
        this.jobType = jobType;
        if (jobType != JobType.MV) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                throw new AnalysisException("only ADMIN priv can operate");
            }
        }
    }

    public static Integer getColumnIndexFromColumnName(String columnName, TMetadataTableRequestParams params)
            throws org.apache.doris.common.AnalysisException {
        if (!params.isSetTasksMetadataParams()) {
            throw new org.apache.doris.common.AnalysisException("Tasks metadata params is not set.");
        }
        TTasksMetadataParams taskMetadataParams = params.getTasksMetadataParams();
        String type = taskMetadataParams.getType();
        JobType jobType = JobType.valueOf(type.toUpperCase());
        if (jobType == null) {
            throw new AnalysisException("Invalid task metadata query");
        }
        if (JobType.MV == jobType) {
            return MTMVTask.COLUMN_TO_INDEX.get(columnName.toLowerCase());
        } else if (JobType.INSERT == jobType) {
            return InsertTask.COLUMN_TO_INDEX.get(columnName.toLowerCase());
        } else {
            throw new AnalysisException("Invalid job type: " + jobType.toString());
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.TASKS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.TASKS);
        TTasksMetadataParams taskParam = new TTasksMetadataParams();
        taskParam.setType(jobType.name());
        taskParam.setCurrentUserIdent(ConnectContext.get().getCurrentUserIdentity().toThrift());
        metaScanRange.setTasksParams(taskParam);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "TasksTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        if (JobType.MV == jobType) {
            return MTMVTask.SCHEMA;
        } else if (JobType.INSERT == jobType) {
            return InsertTask.SCHEMA;
        } else {
            throw new AnalysisException("Invalid job type: " + jobType.toString());
        }
    }
}


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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

@Log4j2
public class InsertTask extends AbstractInsertTask {

    public static final ImmutableList<Column> SCHEMA = BASE_SCHEMA;

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }


    /**
     * task merge type
     */
    enum MergeType {
        MERGE,
        APPEND,
        DELETE
    }

    /**
     * task type
     */
    enum TaskType {
        UNKNOWN, // this is only for ISSUE #2354
        PENDING,
        LOADING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    public InsertTask(InsertIntoTableCommand insertInto,
                      ConnectContext ctx, StmtExecutor executor, LoadStatistic statistic) {
        this(insertInto.getLabelName().get(), insertInto, ctx, executor, statistic);
    }

    public InsertTask(String currentDb, String sql, UserIdentity userIdentity) {
        this.sql = sql;
        this.currentDb = currentDb;
        this.userIdentity = userIdentity;
    }

    public InsertTask(String labelName, String currentDb, String sql, UserIdentity userIdentity) {
        this.labelName = labelName;
        this.sql = sql;
        this.currentDb = currentDb;
        this.userIdentity = userIdentity;
    }

    public InsertTask(String labelName, InsertIntoTableCommand insertInto,
                      ConnectContext ctx, StmtExecutor executor, LoadStatistic statistic) {
        this.labelName = labelName;
        this.command = insertInto;
        this.userIdentity = ctx.getCurrentUserIdentity();
        this.ctx = ctx;
        this.stmtExecutor = executor;
        this.loadStatistic = statistic;
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        if (jobInfo == null) {
            // if task not start, load job is null,return pending task show info
            return getPendingTaskTVFInfo(jobName);
        }
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(jobInfo.getId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(jobInfo.getState().name()));
        trow.addToColumnValue(new TCell().setStringVal(sql));
        // err msg
        String errorMsg = "";
        if (failMsg != null) {
            errorMsg = failMsg.getMsg();
        }
        if (StringUtils.isNotBlank(getErrMsg())) {
            errorMsg = getErrMsg();
        }
        trow.addToColumnValue(new TCell().setStringVal(errorMsg));
        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getFinishTimeMs())));
        // tracking url
        trow.addToColumnValue(new TCell().setStringVal(trackingUrl));
        if (null != jobInfo.getLoadStatistic()) {
            trow.addToColumnValue(new TCell().setStringVal(jobInfo.getLoadStatistic().toJson()));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(""));
        }
        if (userIdentity == null) {
            trow.addToColumnValue(new TCell().setStringVal(""));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        }
        return trow;
    }

    // if task not start, load job is null,return pending task show info
    private TRow getPendingTaskTVFInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(getStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(sql));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        return trow;
    }

}

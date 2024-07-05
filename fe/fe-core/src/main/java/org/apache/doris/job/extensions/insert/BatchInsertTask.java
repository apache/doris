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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.nereids.trees.plans.commands.info.SplitColumnInfo;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.Range;

public class BatchInsertTask extends AbstractInsertTask {
    public static final ImmutableList<Column> BATCH_INSERT_SCHEMA = ImmutableList.<Column>builder().addAll(BASE_SCHEMA)
            .add(new Column("SplitRange", ScalarType.createStringType())).build();
    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
        for (int i = 0; i < BATCH_INSERT_SCHEMA.size(); i++) {
            builder.put(BATCH_INSERT_SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    //do not need to serialize
    private SplitColumnInfo splitColumnInfo;

    private Range splitRange;
    @SerializedName("ssp")
    private Object starts;
    @SerializedName("esp")
    private Object ends;

    public BatchInsertTask(SplitColumnInfo splitColumnInfo, Range splitRange, String currentDb, String sql,
                           UserIdentity userIdentity) {
        this.splitColumnInfo = splitColumnInfo;
        this.splitRange = splitRange;
        this.sql = sql;
        this.currentDb = currentDb;
        this.userIdentity = userIdentity;
        this.labelName = "BatchInsertTask";
        this.starts = splitRange.getMinimum();
        this.ends = splitRange.getMaximum();

    }

    @Override
    public void before() throws JobException {
        super.before();
        this.command.initSplitRange(splitColumnInfo, splitRange);
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = super.getTvfInfo(jobName);
        if (null == starts || null == ends) {
            trow.addToColumnValue(new TCell().setStringVal(null));
            return trow;
        }
        String ranges = String.format("[%s,%s]", starts, ends);
        trow.addToColumnValue(new TCell().setStringVal(ranges));
        return trow;
    }
}

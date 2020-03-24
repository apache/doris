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

package org.apache.doris.common.proc;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.MaterializedViewHandler;
import org.apache.doris.alter.RollupJobV2;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class RollupProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishedTime")
            .add("BaseIndexName").add("RollupIndexName").add("RollupId").add("TransactionId")
            .add("State").add("Msg").add("Progress").add("Timeout")
            .build();

    private MaterializedViewHandler materializedViewHandler;
    private Database db;

    public RollupProcDir(MaterializedViewHandler materializedViewHandler, Database db) {
        this.materializedViewHandler = materializedViewHandler;
        this.db = db;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(materializedViewHandler);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> rollupJobInfos = materializedViewHandler.getAlterJobInfosByDb(db);
        for (List<Comparable> infoStr : rollupJobInfos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobIdStr)) {
            throw new AnalysisException("Job id is null");
        }

        long jobId = -1L;
        try {
            jobId = Long.valueOf(jobIdStr);
        } catch (Exception e) {
            throw new AnalysisException("Job id is invalid");
        }

        Preconditions.checkState(jobId != -1L);
        AlterJobV2 job = materializedViewHandler.getUnfinishedAlterJobV2ByJobId(jobId);
        if (job == null) {
            return null;
        }

        return new RollupJobProcNode((RollupJobV2) job);
    }

}

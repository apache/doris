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

import org.apache.doris.analysis.ShowRoutineLoadTaskStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;

/*
    SHOW RPOC "/routine_loads/{jobName}/{jobId}"
    show routine load task info belong to job

    RESULT:
    show result is sames as show routine load task
 */
public class RoutineLoadProcNode implements ProcNodeInterface {

    private final long jobId;

    public RoutineLoadProcNode(long jobId) {
        this.jobId = jobId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        // check job id
        RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();
        RoutineLoadJob routineLoadJob = routineLoadManager.getJob(jobId);
        if (routineLoadJob == null) {
            throw new AnalysisException("Job[" + jobId + "] does not exist");
        }

        BaseProcResult result = new BaseProcResult();
        result.setNames(ShowRoutineLoadTaskStmt.getTitleNames());
        result.setRows(routineLoadJob.getTasksShowInfo());
        return result;
    }
}

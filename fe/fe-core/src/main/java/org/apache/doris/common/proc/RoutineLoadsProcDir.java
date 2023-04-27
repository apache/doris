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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

/*
    SHOW PROC "/routine_loads"
    show statistic of all of running loads

    RESULT:
    Name | Id | DbName | JobStatistic | TaskStatistic
 */
public class RoutineLoadsProcDir implements ProcDirInterface {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Name")
                    .add("Id")
                    .add("DbName")
                    .add("Statistic")
                    .add("TaskStatistic")
                    .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobName) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobName)) {
            throw new IllegalArgumentException("job name could not be empty of null");
        }
        return new RoutineLoadsNameProcDir(jobName);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult baseProcResult = new BaseProcResult();
        baseProcResult.setNames(TITLE_NAMES);
        RoutineLoadManager routineLoadManager = Env.getCurrentEnv().getRoutineLoadManager();
        try {
            List<RoutineLoadJob> routineLoadJobList = routineLoadManager.getJob(null, null, true, null);
            for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
                baseProcResult.addRow(routineLoadJob.getShowStatistic());
            }
        } catch (MetaNotFoundException e) {
            throw new AnalysisException("failed to get all of routine load job");
        }
        return baseProcResult;
    }
}

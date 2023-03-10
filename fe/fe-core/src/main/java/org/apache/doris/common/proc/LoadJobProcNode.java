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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.loadv2.LoadManager;

import com.google.common.collect.ImmutableList;

public class LoadJobProcNode implements ProcNodeInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BackendId").add("TabletId").add("ReplicaId").add("Version")
            .add("PartitionId").add("LoadVersion")
            .build();

    private LoadManager loadManager;
    private long jobId;

    public LoadJobProcNode(LoadManager loadManager, long jobId) {
        this.loadManager = loadManager;
        this.jobId = jobId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        // TODO get results from LoadManager. Before do that, update implement of LoadManager：record detail info
        return result;
    }

}

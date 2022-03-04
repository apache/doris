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
import org.apache.doris.load.Load;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class LoadJobProcNode implements ProcNodeInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BackendId").add("TabletId").add("ReplicaId").add("Version")
            .add("PartitionId").add("LoadVersion")
            .build();

    private Load load;
    private long jobId;

    public LoadJobProcNode(Load load, long jobId) {
        this.load = load;
        this.jobId = jobId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> infos = load.getLoadJobUnfinishedInfo(jobId);
        // In this step, the detail of load job which is belongs to LoadManager will not be presented.
        // The reason is that there are no detail info in load job which is streaming during loading.
        // So it don't need to invoke the LoadManager here.
        for (List<Comparable> info : infos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : info) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

}

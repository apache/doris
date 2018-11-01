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

public class DeleteJobProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId")
            .build();

    private long jobId;
    private Load load;

    public DeleteJobProcNode(Load load, long jobId) {
        this.jobId = jobId;
        this.load = load;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> infos = load.getAsyncDeleteJobInfo(jobId);
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
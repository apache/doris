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
import org.apache.doris.load.DeleteHandler;

import com.google.common.collect.ImmutableList;
import org.apache.doris.load.Load;

import java.util.ArrayList;
import java.util.List;

public class DeleteInfoProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("jobId").add("TableId").add("TableName").add("PartitionId")
            .add("PartitionName").add("CreateTime").add("DeleteCondition").add("Version")
            .add("VersionHash").add("State")
            .build();

    public static final ImmutableList<String> TITLE_NAMES_FOR_USER = new ImmutableList.Builder<String>()
            .add("TableName").add("PartitionName").add("CreateTime").add("DeleteCondition")
            .add("State")
            .build();

    private Load load;
    private DeleteHandler deleteHandler;
    private long dbId;

    public DeleteInfoProcDir(DeleteHandler deleteHandler, Load load, long dbId) {
        this.load = load;
        this.deleteHandler = deleteHandler;
        this.dbId = dbId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> infos = deleteHandler.getDeleteInfosByDb(dbId, false);
        infos.addAll(load.getDeleteInfosByDb(dbId, false));
        for (List<Comparable> info : infos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : info) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobIdStr) throws AnalysisException {
        long jobId = -1L;
        try {
            jobId = Long.valueOf(jobIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid job id format: " + jobIdStr);
        }

        // return new DeleteJobProcNode(load, jobId);
        return null;
    }

}
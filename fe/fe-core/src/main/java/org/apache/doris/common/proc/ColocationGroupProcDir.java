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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/*
 * show proc "/colocation_group";
 */
public class ColocationGroupProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("GroupId").add("GroupName").add("TableIds")
            .add("BucketsNum").add("ReplicaAllocation").add("DistCols").add("IsStable")
            .add("ErrorMsg").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String groupIdStr) throws AnalysisException {
        String[] parts = groupIdStr.split("\\.");
        if (parts.length != 2) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        long dbId = -1;
        long grpId = -1;
        try {
            dbId = Long.valueOf(parts[0]);
            grpId = Long.valueOf(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        GroupId groupId = new GroupId(dbId, grpId);
        
        // Check if running in Cloud Mode
        if (Config.isCloudMode()) {
            return lookupForCloud(groupId);
        } else {
            return lookupForLocal(groupId);
        }
    }

    private ProcNodeInterface lookupForCloud(GroupId groupId) {
        // Logic for Cloud Mode: Get BE distribution from CloudSystemInfoService
        CloudSystemInfoService infoService = (CloudSystemInfoService) Env.getCurrentSystemInfo();
        
        // Get the colocate index from cloud
        ColocateTableIndex index = infoService.getColocateIndex();
        
        // 获取这个 group 的每个 bucket 对应的 backend 序列
        // 注意：这里应该和 Local Mode 用相同的数据结构
        Map<org.apache.doris.resource.Tag, List<List<Long>>> beSeqs = index.getBackendsPerBucketSeq(groupId);
        
        // 如果没有找到数据，返回空的结果
        if (beSeqs == null || beSeqs.isEmpty()) {
            return new ColocationGroupBackendSeqsProcNode(Maps.newHashMap());
        }
        
        return new ColocationGroupBackendSeqsProcNode(beSeqs);
    }

    private ProcNodeInterface lookupForLocal(GroupId groupId) {
        // Original Logic for Local Mode
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Map<org.apache.doris.resource.Tag, List<List<Long>>> beSeqs = index.getBackendsPerBucketSeq(groupId);
        return new ColocationGroupBackendSeqsProcNode(beSeqs);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        List<List<String>> infos = index.getInfos();
        result.setRows(infos);
        return result;
    }
}

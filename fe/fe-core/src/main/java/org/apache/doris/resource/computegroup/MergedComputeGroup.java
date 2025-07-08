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

package org.apache.doris.resource.computegroup;

import org.apache.doris.common.UserException;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupKey;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

public class MergedComputeGroup extends ComputeGroup {

    private Set<String> computeGroupSet;

    public MergedComputeGroup(String mergedName, Set<String> computeGroupSet, SystemInfoService systemInfoService) {
        super(mergedName, mergedName, systemInfoService);
        this.computeGroupSet = computeGroupSet;
    }

    @Override
    public boolean containsBackend(String computeGroupName) {
        return computeGroupSet.contains(computeGroupName);
    }

    @Override
    public ImmutableList<Backend> getBackendList() {
        return systemInfoService.getBackendListByComputeGroup(computeGroupSet);
    }

    public List<WorkloadGroup> getWorkloadGroup(String wgName, WorkloadGroupMgr wgMgr) throws UserException {
        List<WorkloadGroup> wgList = Lists.newArrayList();

        for (String cgName : computeGroupSet) {
            WorkloadGroup wg = wgMgr.getWorkloadGroupByComputeGroup(WorkloadGroupKey.get(cgName, wgName));
            if (wg == null) {
                throw new UserException("Can not find workload group " + wgName + " in compute group " + cgName);
            }
            wgList.add(wg);
        }
        return wgList;
    }

    @Override
    public String toString() {
        return String.format("%s name=%s ", MergedComputeGroup.class.getSimpleName(), name);
    }

}

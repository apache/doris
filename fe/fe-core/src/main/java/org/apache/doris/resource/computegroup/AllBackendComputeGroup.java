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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.resource.workloadgroup.WorkloadGroupKey;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class AllBackendComputeGroup extends ComputeGroup {

    public AllBackendComputeGroup(SystemInfoService systemInfoService) {
        super(AllBackendComputeGroup.class.getSimpleName(), AllBackendComputeGroup.class.getSimpleName(),
                systemInfoService);
    }

    @Override
    public boolean containsBackend(String beTag) {
        if (Config.isCloudMode()) {
            throw new RuntimeException("AllBackendComputeGroup not implements containsBackend in cloud mode.");
        }
        // currently AllBackendComputeGroup is used when admin/root user not specifies a resource tag,
        // then they can get all backends
        return true;
    }

    @Override
    public String getId() {
        throw new RuntimeException("AllBackendComputeGroup not implements getId.");
    }

    @Override
    public String getName() {
        throw new RuntimeException("AllBackendComputeGroup not implements getName.");
    }

    @Override
    public ImmutableList<Backend> getBackendList() {
        return systemInfoService.getAllClusterBackendsNoException().values().asList();
    }

    // In non cloud mode, if root or admin user not set resource group name, then will use all
    // resource groups(This is by design in the past). So we need get all workload groups
    // from different resource groups and publish the workload group to all backends.
    // The backend will filter the workload group that is pre-created in its local.
    @Override
    public List<WorkloadGroup> getWorkloadGroup(String wgName, WorkloadGroupMgr wgMgr) throws UserException {
        List<WorkloadGroup> wgList = Lists.newArrayList();
        Collection<Backend> beList = systemInfoService.getAllClusterBackendsNoException().values();
        if (beList.size() == 0) {
            throw new RuntimeException("No backend available for Workload Group " + wgName);
        }
        Set<String> cgSet = Sets.newHashSet();
        for (Backend backend : beList) {
            // in cloud mode, name is cluster id.
            // in no-cloud mode, name is resource tag's name
            String computeGroup = backend.getComputeGroup();
            if (cgSet.contains(computeGroup)) {
                continue;
            }
            cgSet.add(computeGroup);
            WorkloadGroup wg = wgMgr.getWorkloadGroupByComputeGroup(
                    WorkloadGroupKey.get(computeGroup, wgName));
            if (wg == null) {
                if (Config.isCloudMode()) {
                    throw new UserException(
                            "Can not find workload group " + wgName + " in compute croup "
                                    + backend.getCloudClusterName());
                } else {
                    throw new UserException(
                            "Can not find workload group " + wgName + " in compute group " + computeGroup);
                }
            }
            wgList.add(wg);
        }
        return wgList;
    }

    @Override
    public String toString() {
        return AllBackendComputeGroup.class.getSimpleName();
    }

}

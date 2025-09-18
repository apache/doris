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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComputeGroup {

    private static final Logger LOG = LogManager.getLogger(ComputeGroup.class);

    // NOTE: Why need an invalid compute group
    // invalid compute group is mainly used in local-mode(not cloud mode),
    // it comes from invalid resource tag(UserPropertyMgr.getComputeGroup), when trying to find a resource tag
    // by user, but the user has not a property(maybe user is invalid),
    // so introduce an invalid compute group to cover this case.
    // caller should deal the invalid compute case by case;
    // eg, if routine load cant not find a valid compute group when finding Slot BE, it could find BE by replica's tag.
    // some other case may throw an Exception.
    public static final String INVALID_COMPUTE_GROUP_NAME = "_invalid_compute_group_name";

    public static final ComputeGroup INVALID_COMPUTE_GROUP = new ComputeGroup(INVALID_COMPUTE_GROUP_NAME,
            INVALID_COMPUTE_GROUP_NAME, null);

    public static final String INVALID_COMPUTE_GROUP_ERR_MSG
            = "can not find a valid compute group, please check whether current user is valid.";

    protected SystemInfoService systemInfoService;

    protected String id;

    protected String name;

    public ComputeGroup(String id, String name, SystemInfoService systemInfoService) {
        this.id = id;
        this.name = name;
        this.systemInfoService = systemInfoService;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean containsBackend(String backendTag) {
        checkInvalidComputeGroup();
        return StringUtils.equals(this.name, backendTag);
    }

    public List<Backend> getBackendList() {
        checkInvalidComputeGroup();
        Set<String> cgSet = new HashSet<>();
        cgSet.add(name);
        return systemInfoService.getBackendListByComputeGroup(cgSet);
    }

    @Override
    public String toString() {
        return String.format("%s id=%s, name=%s", this.getClass().getSimpleName(), id, name);
    }

    @Override
    public boolean equals(Object obj) {
        // NOTE: currently equals is used to compare INVALID_COMPUTE_GROUP, just using ```==``` is enough.
        return (this == obj);
    }

    // use wgMgr as args is just for FE UT, otherwise get wgMgr from env is hard to mock
    public List<WorkloadGroup> getWorkloadGroup(String wgName, WorkloadGroupMgr wgMgr) throws UserException {
        WorkloadGroup wg = wgMgr
                .getWorkloadGroupByComputeGroup(WorkloadGroupKey.get(id, wgName));
        if (wg == null) {
            throw new UserException("Can not find workload group " + wgName + " in compute croup " + name);
        }
        return Lists.newArrayList(wg);
    }

    private void checkInvalidComputeGroup() {
        if (this.getName() == INVALID_COMPUTE_GROUP_NAME) {
            throw new RuntimeException(
                    "invalid compute group can not be used, please check whether current user is valid.");
        }
    }

}


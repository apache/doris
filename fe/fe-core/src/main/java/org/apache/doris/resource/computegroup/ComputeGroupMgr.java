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

import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

public class ComputeGroupMgr {

    private SystemInfoService systemInfoService;

    public ComputeGroupMgr(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    public ComputeGroup getComputeGroupByName(String name) throws UserException {
        if (Config.isCloudMode()) {
            CloudSystemInfoService cloudSystemInfoService = (CloudSystemInfoService) systemInfoService;
            String clusterId = cloudSystemInfoService.getCloudClusterIdByName(name);
            if (StringUtils.isEmpty(clusterId)) {
                throw new UserException("Can not find compute group:" + name);
            }
            return new CloudComputeGroup(clusterId, name, cloudSystemInfoService);
        } else {
            return new ComputeGroup(name, name, systemInfoService);
        }
    }

    public ComputeGroup getComputeGroup(Set<Tag> rgTags) {
        Set<String> tagStrSet = Sets.newHashSet();
        for (Tag tag : rgTags) {
            tagStrSet.add(tag.value);
        }
        return new MergedComputeGroup(String.join(",", tagStrSet), tagStrSet, systemInfoService);
    }

    // to be compatible with resource tag's logic, if root/admin user not specify a resource tag,
    // which means return all backends.
    public ComputeGroup getAllBackendComputeGroup() {
        return new AllBackendComputeGroup(systemInfoService);
    }

    public Set<String> getAllComputeGroupIds() {
        Set<String> ret = Sets.newHashSet();
        for (Backend backend : systemInfoService.getAllClusterBackendsNoException().values()) {
            ret.add(backend.getComputeGroup());
        }
        return ret;
    }

}

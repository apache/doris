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

import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;

import java.util.Set;

public class MergedComputeGroup extends ComputeGroup {

    private Set<String> computeGroupSet;

    public MergedComputeGroup(Set<String> computeGroupSet, SystemInfoService systemInfoService) {
        super(MergedComputeGroup.class.getSimpleName(), MergedComputeGroup.class.getSimpleName(), systemInfoService);
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

    @Override
    public String getId() {
        throw new RuntimeException("MergedComputeGroup not implements getId.");
    }

    @Override
    public String getName() {
        throw new RuntimeException("MergedComputeGroup not implements getName.");
    }

    // current main for UT
    public Set<String> getComputeGroupNameSet() {
        return computeGroupSet;
    }

    @Override
    public String toString() {
        return String.format("%s %s", MergedComputeGroup.class.getSimpleName(), String.join(",", computeGroupSet));
    }

}

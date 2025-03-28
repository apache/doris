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
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableList;

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

    @Override
    public String toString() {
        return AllBackendComputeGroup.class.getSimpleName();
    }

}

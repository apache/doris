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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComputeGroup {

    private static final Logger LOG = LogManager.getLogger(ComputeGroup.class);

    protected SystemInfoService systemInfoService;

    protected String id;

    protected String name;

    protected ComputeGroup(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    protected ComputeGroup(String id, String name) {
        this.id = id;
        this.name = name;
    }

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

    public List<Backend> getBackendList() {
        Set<String> cgSet = new HashSet<>();
        cgSet.add(name);
        return systemInfoService.getBackendListByComputeGroup(cgSet);
    }

}


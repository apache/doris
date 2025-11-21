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

import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

public class ComputeGroupMgr {

    private SystemInfoService systemInfoService;

    public ComputeGroupMgr(SystemInfoService systemInfoService) {
        this.systemInfoService = systemInfoService;
    }

    public static String computeGroupNotFoundPromptMsg(String physicalClusterName) {
        StringBuilder sb = new StringBuilder();
        Pair<String, String> computeGroupInfos = ConnectContext.computeGroupFromHintMsg();
        sb.append(" Unable to find the compute group: ");
        sb.append("<");
        if (physicalClusterName == null) {
            sb.append(computeGroupInfos.first);
        } else {
            sb.append(physicalClusterName);
        }
        sb.append(">");
        sb.append(". Please check if the compute group has been deleted. how this compute group is selected: ");
        sb.append(computeGroupInfos.second);
        return sb.toString();
    }
}

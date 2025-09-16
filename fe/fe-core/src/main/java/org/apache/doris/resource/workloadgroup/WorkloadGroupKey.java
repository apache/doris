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

package org.apache.doris.resource.workloadgroup;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class WorkloadGroupKey {

    private String computeGroup;

    private String workloadGroupName;

    private WorkloadGroupKey(String computeGroup, String workloadGroupName) {
        this.computeGroup = computeGroup;
        this.workloadGroupName = workloadGroupName;
    }

    public static WorkloadGroupKey get(String computeGroup, String workloadGroupName) {
        Preconditions.checkState(!StringUtils.isEmpty(workloadGroupName));
        return new WorkloadGroupKey(
                StringUtils.isEmpty(computeGroup) ? WorkloadGroupMgr.EMPTY_COMPUTE_GROUP : computeGroup,
                workloadGroupName);
    }

    public String getComputeGroup() {
        return computeGroup;
    }

    public String getWorkloadGroupName() {
        return workloadGroupName;
    }

    @Override
    public String toString() {
        return computeGroup + "." + workloadGroupName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof WorkloadGroupKey) {
            WorkloadGroupKey otherWgKey = (WorkloadGroupKey) o;

            boolean cgEqual = this.computeGroup.equals(otherWgKey.computeGroup);
            boolean wgEqual = this.workloadGroupName.equals(otherWgKey.workloadGroupName);
            return cgEqual && wgEqual;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(computeGroup, workloadGroupName);
    }

}

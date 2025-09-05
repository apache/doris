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

package org.apache.doris.cloud.catalog;

import org.apache.doris.cloud.proto.Cloud;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ComputeGroup {
    private static final Logger LOG = LogManager.getLogger(ComputeGroup.class);

    private enum PolicyTypeEnum {
        ActiveStandby,
    }

    public enum ComputeTypeEnum {
        SQL,
        COMPUTE,
        VIRTUAL,
    }

    @Getter
    @Setter
    public static class Policy {
        PolicyTypeEnum policyType;
        String activeComputeGroup;
        String standbyComputeGroup;
        long failoverFailureThreshold = 3;
        long unhealthyNodeThresholdPercent = 100;
        List<String> cacheWarmupJobIds = new ArrayList<>();

        public Policy() {
            policyType = PolicyTypeEnum.ActiveStandby;
        }

        @Override
        public String toString() {
            Map<String, String> showMap = new LinkedHashMap<>();
            showMap.put("policyType", policyType.toString());
            showMap.put("activeComputeGroup", activeComputeGroup);
            showMap.put("standbyComputeGroup", standbyComputeGroup);
            showMap.put("failoverFailureThreshold", String.valueOf(failoverFailureThreshold));
            showMap.put("unhealthyNodeThresholdPercent", String.valueOf(unhealthyNodeThresholdPercent));
            showMap.put("cacheWarmupJobIds", String.valueOf(cacheWarmupJobIds));
            Gson gson = new Gson();
            return gson.toJson(showMap);
        }

        public Cloud.ClusterPolicy toPb() {
            return Cloud.ClusterPolicy.newBuilder()
                    .setType(Cloud.ClusterPolicy.PolicyType.ActiveStandby)
                    .setActiveClusterName(activeComputeGroup)
                    .addStandbyClusterNames(standbyComputeGroup)
                    .setFailoverFailureThreshold(failoverFailureThreshold)
                    .setUnhealthyNodeThresholdPercent(unhealthyNodeThresholdPercent)
                    .build();
        }
    }

    @Getter
    private String id;
    @Getter
    // cg can be renamed
    @Setter
    private String name;
    @Getter
    private ComputeTypeEnum type;

    // record sub cluster name
    @Getter
    @Setter
    private List<String> subComputeGroups;
    @Getter
    @Setter
    private long unavailableSince = -1;
    @Getter
    @Setter
    private long availableSince = -1;
    @Getter
    @Setter
    private Policy policy;
    @Getter
    @Setter
    private String currentClusterName;
    @Getter
    @Setter
    private boolean needRebuildFileCache = false;

    public ComputeGroup(String id, String name, ComputeTypeEnum type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }

    public boolean isVirtual() {
        return type == ComputeTypeEnum.VIRTUAL;
    }

    public String getActiveComputeGroup() {
        if (policy == null) {
            return "empty_policy";
        }
        return policy.getActiveComputeGroup();
    }

    public String getStandbyComputeGroup() {
        if (policy == null) {
            return "empty_policy";
        }
        return policy.getStandbyComputeGroup();
    }

    @Override
    public String toString() {
        Map<String, String> showMap = new LinkedHashMap<>();
        showMap.put("id", id);
        showMap.put("name", name);
        showMap.put("type", type.toString());
        showMap.put("unavailableSince", String.valueOf(unavailableSince));
        showMap.put("availableSince", String.valueOf(availableSince));
        showMap.put("policy", policy == null ? "no_policy" : policy.toString());
        Gson gson = new Gson();
        return gson.toJson(showMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComputeGroup)) {
            return false;
        }
        ComputeGroup that = (ComputeGroup) o;
        return unavailableSince == that.unavailableSince
                && availableSince == that.availableSince
                && id.equals(that.id)
                && name.equals(that.name)
                && type == that.type
                && (policy != null ? policy.equals(that.policy) : that.policy == null);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (int) (unavailableSince ^ (unavailableSince >>> 32));
        result = 31 * result + (int) (availableSince ^ (availableSince >>> 32));
        result = 31 * result + (policy != null ? policy.hashCode() : 0);
        return result;
    }
}

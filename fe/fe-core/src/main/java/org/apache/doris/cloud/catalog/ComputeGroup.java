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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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

    public static final String BALANCE_TYPE = "balance_type";

    public static final String BALANCE_WARM_UP_TASK_TIMEOUT = "balance_warm_up_task_timeout";

    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>()
            .add(BALANCE_TYPE).add(BALANCE_WARM_UP_TASK_TIMEOUT).build();

    private static final Map<String, String> ALL_PROPERTIES_DEFAULT_VALUE_MAP = Maps.newHashMap();

    public static final int DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT = Config.cloud_pre_heating_time_limit_sec;
    public static final BalanceTypeEnum DEFAULT_COMPUTE_GROUP_BALANCE_ENUM
            = BalanceTypeEnum.fromString(Config.cloud_warm_up_for_rebalance_type);

    static {
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(BALANCE_TYPE, DEFAULT_COMPUTE_GROUP_BALANCE_ENUM.getValue());
        if (BalanceTypeEnum.ASYNC_WARMUP.getValue().equals(Config.cloud_warm_up_for_rebalance_type)) {
            ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(BALANCE_WARM_UP_TASK_TIMEOUT,
                    String.valueOf(DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));
        }
    }

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

    @Getter
    @Setter
    private Map<String, String> properties = new LinkedHashMap<>(ALL_PROPERTIES_DEFAULT_VALUE_MAP);

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

    private void validateTimeoutRestriction(Map<String, String> inputProperties) throws DdlException {
        if (!properties.containsKey(BALANCE_TYPE)) {
            return;
        }
        String originalBalanceType = properties.get(BALANCE_TYPE);
        if (BalanceTypeEnum.ASYNC_WARMUP.getValue().equals(originalBalanceType)) {
            return;
        }

        if (inputProperties.containsKey(BALANCE_TYPE)
                && BalanceTypeEnum.ASYNC_WARMUP.getValue().equals(inputProperties.get(BALANCE_TYPE))) {
            return;
        }

        if (inputProperties.containsKey(BALANCE_WARM_UP_TASK_TIMEOUT)) {
            throw new DdlException("Property " + BALANCE_WARM_UP_TASK_TIMEOUT
                    + " cannot be set when current " + BALANCE_TYPE + " is " + originalBalanceType
                    + ". Only async_warmup type supports timeout setting.");
        }
    }

    /**
     * Validate a single property key-value pair.
     */
    private static void validateProperty(String key, String value) throws DdlException {
        if (value == null || value.isEmpty()) {
            return;
        }

        if (!ALL_PROPERTIES_NAME.contains(key)) {
            throw new DdlException("Property " + key + " is not supported");
        }

        // Validate specific properties
        if (BALANCE_TYPE.equals(key)) {
            if (!BalanceTypeEnum.isValid(value)) {
                throw new DdlException("Property " + BALANCE_TYPE
                    + " only support without_warmup or async_warmup or sync_warmup");
            }
        } else if (BALANCE_WARM_UP_TASK_TIMEOUT.equals(key)) {
            try {
                int timeout = Integer.parseInt(value);
                if (timeout <= 0) {
                    throw new DdlException("Property " + BALANCE_WARM_UP_TASK_TIMEOUT + " must be positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException("Property " + BALANCE_WARM_UP_TASK_TIMEOUT + " must be positive integer");
            }
        }
    }

    public void checkProperties(Map<String, String> inputProperties) throws DdlException {
        if (inputProperties == null || inputProperties.isEmpty()) {
            return;
        }

        for (Map.Entry<String, String> entry : inputProperties.entrySet()) {
            validateProperty(entry.getKey(), entry.getValue());
        }

        validateTimeoutRestriction(inputProperties);
    }

    public void modifyProperties(Map<String, String> inputProperties) throws DdlException {
        String balanceType = inputProperties.get(BALANCE_TYPE);
        if (balanceType == null) {
            return;
        }
        if (BalanceTypeEnum.WITHOUT_WARMUP.getValue().equals(balanceType)
                || BalanceTypeEnum.SYNC_WARMUP.getValue().equals(balanceType)
                || BalanceTypeEnum.PEER_READ_ASYNC_WARMUP.getValue().equals(balanceType)) {
            // delete BALANCE_WARM_UP_TASK_TIMEOUT if exists
            properties.remove(BALANCE_WARM_UP_TASK_TIMEOUT);
        } else if (BalanceTypeEnum.ASYNC_WARMUP.getValue().equals(balanceType)) {
            // if BALANCE_WARM_UP_TASK_TIMEOUT exists, it has been validated in validateProperty
            if (!properties.containsKey(BALANCE_WARM_UP_TASK_TIMEOUT)) {
                properties.put(BALANCE_WARM_UP_TASK_TIMEOUT, String.valueOf(DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));
            }
        }
    }

    // set properties, just set in periodic instance status checker
    public void setProperties(Map<String, String> propertiesInMs) {
        if (propertiesInMs == null || propertiesInMs.isEmpty()) {
            return;
        }

        for (Map.Entry<String, String> entry : propertiesInMs.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            try {
                validateProperty(key, value);
            } catch (DdlException e) {
                LOG.warn("ignore invalid property. compute group: {}, key: {}, value: {}, error: {}",
                        name, key, value, e.getMessage());
                continue;
            }

            if (value != null && !value.isEmpty()) {
                properties.put(key, value);
            }
        }
    }

    public BalanceTypeEnum getBalanceType() {
        String balanceType = properties.get(BALANCE_TYPE);
        BalanceTypeEnum type = BalanceTypeEnum.fromString(balanceType);
        if (type == null) {
            return BalanceTypeEnum.ASYNC_WARMUP;
        }
        return type;
    }

    public int getBalanceWarmUpTaskTimeout() {
        String timeoutStr = properties.get(BALANCE_WARM_UP_TASK_TIMEOUT);
        try {
            return Integer.parseInt(timeoutStr);
        } catch (NumberFormatException e) {
            return DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT;
        }
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
        showMap.put("properties", properties.toString());
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

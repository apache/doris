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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.NotifyUpdateStoragePolicyTask;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * S3 resource for olap table
 *
 * Syntax:
 * CREATE RESOURCE "remote_s3"
 * PROPERTIES
 * (
 *    "type" = "s3",
 *    "s3_endpoint" = "bj",
 *    "s3_region" = "bj",
 *    "s3_root_path" = "/path/to/root",
 *    "s3_access_key" = "bbb",
 *    "s3_secret_key" = "aaaa",
 *    "s3_max_connections" = "50",
 *    "s3_request_timeout_ms" = "3000",
 *    "s3_connection_timeout_ms" = "1000"
 * );
 */
public class S3Resource extends Resource {
    private static final Logger LOG = LogManager.getLogger(S3Resource.class);
    // required
    public static final String S3_ENDPOINT = "s3_endpoint";
    public static final String S3_REGION = "s3_region";
    public static final String S3_ROOT_PATH = "s3_root_path";
    public static final String S3_ACCESS_KEY = "s3_access_key";
    public static final String S3_SECRET_KEY = "s3_secret_key";
    public static final String S3_BUCKET = "s3_bucket";

    // optional
    public static final String S3_MAX_CONNECTIONS = "s3_max_connections";
    public static final String S3_REQUEST_TIMEOUT_MS = "s3_request_timeout_ms";
    public static final String S3_CONNECTION_TIMEOUT_MS = "s3_connection_timeout_ms";
    public static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
    public static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
    public static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    @SerializedName(value = "usedByPolicySet")
    private Set<String> usedByPolicySet;

    public boolean policyAddToSet(final String policeName) {
        boolean flag = this.usedByPolicySet.add(policeName);
        if (flag) {
            // log set
            Env.getCurrentEnv().getEditLog().logAlterResource(this);
            LOG.info("{} policy add s3 resource, current set: {}", policeName, usedByPolicySet);
        }
        return flag;
    }

    public S3Resource(String name) {
        this(name, Maps.newHashMap(), Sets.newHashSet());
    }

    public S3Resource(String name, Map<String, String> properties, Set<String> policySet) {
        super(name, ResourceType.S3);
        this.properties = properties;
        this.usedByPolicySet = policySet;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = properties;
        // check properties
        // required
        checkRequiredProperty(S3_ENDPOINT);
        checkRequiredProperty(S3_REGION);
        checkRequiredProperty(S3_ROOT_PATH);
        checkRequiredProperty(S3_ACCESS_KEY);
        checkRequiredProperty(S3_SECRET_KEY);
        checkRequiredProperty(S3_BUCKET);
        // optional
        checkOptionalProperty(S3_MAX_CONNECTIONS, DEFAULT_S3_MAX_CONNECTIONS);
        checkOptionalProperty(S3_REQUEST_TIMEOUT_MS, DEFAULT_S3_REQUEST_TIMEOUT_MS);
        checkOptionalProperty(S3_CONNECTION_TIMEOUT_MS, DEFAULT_S3_CONNECTION_TIMEOUT_MS);
    }

    private void checkRequiredProperty(String propertyKey) throws DdlException {
        String value = properties.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    private void checkOptionalProperty(String propertyKey, String defaultValue) {
        this.properties.putIfAbsent(propertyKey, defaultValue);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // can't change, because remote fs use it info to find data.
        List<String> cantChangeProperties = Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ROOT_PATH, S3_BUCKET);
        Optional<String> any = cantChangeProperties.stream().filter(properties::containsKey).findAny();
        if (any.isPresent()) {
            throw new DdlException("current not support modify property : " + any.get());
        }
        // modify properties
        replaceIfEffectiveValue(this.properties, S3_ACCESS_KEY, properties.get(S3_ACCESS_KEY));
        replaceIfEffectiveValue(this.properties, S3_SECRET_KEY, properties.get(S3_SECRET_KEY));
        replaceIfEffectiveValue(this.properties, S3_MAX_CONNECTIONS, properties.get(S3_MAX_CONNECTIONS));
        replaceIfEffectiveValue(this.properties, S3_REQUEST_TIMEOUT_MS, properties.get(S3_REQUEST_TIMEOUT_MS));
        replaceIfEffectiveValue(this.properties, S3_CONNECTION_TIMEOUT_MS, properties.get(S3_CONNECTION_TIMEOUT_MS));
        notifyUpdate();
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        // check properties
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.remove(S3_ENDPOINT);
        copiedProperties.remove(S3_REGION);
        copiedProperties.remove(S3_ROOT_PATH);
        copiedProperties.remove(S3_ACCESS_KEY);
        copiedProperties.remove(S3_SECRET_KEY);
        copiedProperties.remove(S3_BUCKET);
        copiedProperties.remove(S3_MAX_CONNECTIONS);
        copiedProperties.remove(S3_REQUEST_TIMEOUT_MS);
        copiedProperties.remove(S3_CONNECTION_TIMEOUT_MS);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown S3 resource properties: " + copiedProperties);
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        return copiedProperties;
    }

    public Set<String> getCopiedUsedByPolicySet() {
        return Sets.newHashSet(usedByPolicySet);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3_SECRET_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }

    private void notifyUpdate() {
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        AgentBatchTask batchTask = new AgentBatchTask();

        Map<String, String> copiedProperties = getCopiedProperties();

        for (Long beId : systemInfoService.getBackendIds(true)) {
            this.usedByPolicySet.forEach(
                    policy -> {
                        List<Policy> policiesByType = Env.getCurrentEnv().getPolicyMgr()
                                .getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                        Optional<Policy> findPolicy = policiesByType.stream()
                                .filter(p -> p.getType() == PolicyTypeEnum.STORAGE && policy.equals(p.getPolicyName()))
                                .findAny();
                        LOG.info("find policy in {} ", policiesByType);
                        if (!findPolicy.isPresent()) {
                            return;
                        }
                        // add policy's coolDown ttl、coolDown data、policy name to map
                        Map<String, String> tmpMap = Maps.newHashMap(copiedProperties);
                        StoragePolicy used = (StoragePolicy) findPolicy.get();
                        tmpMap.put(StoragePolicy.COOLDOWN_DATETIME, String.valueOf(used.getCooldownTimestampMs()));

                        final String[] cooldownTtl = {"-1"};
                        Optional.ofNullable(used.getCooldownTtl())
                                .ifPresent(date -> cooldownTtl[0] = used.getCooldownTtl());
                        tmpMap.put(StoragePolicy.COOLDOWN_TTL, cooldownTtl[0]);

                        tmpMap.put(StoragePolicy.MD5_CHECKSUM, used.getMd5Checksum());

                        NotifyUpdateStoragePolicyTask modifyS3ResourcePropertiesTask =
                                new NotifyUpdateStoragePolicyTask(beId, used.getPolicyName(), tmpMap);
                        LOG.info("notify be: {}, policy name: {}, properties: {} to modify S3 resource batch task.",
                                beId, used.getPolicyName(), tmpMap);
                        batchTask.addTask(modifyS3ResourcePropertiesTask);
                    }
            );
        }
        AgentTaskExecutor.submit(batchTask);
    }
}

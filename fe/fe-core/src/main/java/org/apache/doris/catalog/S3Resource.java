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
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * S3 resource
 *
 * Syntax:
 * CREATE RESOURCE "remote_s3"
 * PROPERTIES
 * (
 *    "type" = "s3",
 *    "AWS_ENDPOINT" = "bj",
 *    "AWS_REGION" = "bj",
 *    "AWS_ROOT_PATH" = "/path/to/root",
 *    "AWS_ACCESS_KEY" = "bbb",
 *    "AWS_SECRET_KEY" = "aaaa",
 *    "AWS_MAX_CONNECTION" = "50",
 *    "AWS_REQUEST_TIMEOUT_MS" = "3000",
 *    "AWS_CONNECTION_TIMEOUT_MS" = "1000"
 * );
 */
public class S3Resource extends Resource {
    public enum ReferenceType {
        TVF, // table valued function
        LOAD,
        EXPORT,
        REPOSITORY,
        OUTFILE,
        TABLE,
        POLICY
    }

    private static final Logger LOG = LogManager.getLogger(S3Resource.class);
    public static final String S3_PROPERTIES_PREFIX = "AWS";
    public static final String S3_FS_PREFIX = "fs.s3";
    // required
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";
    public static final String S3_REGION = "AWS_REGION";
    public static final String S3_ACCESS_KEY = "AWS_ACCESS_KEY";
    public static final String S3_SECRET_KEY = "AWS_SECRET_KEY";
    public static final List<String> REQUIRED_FIELDS =
            Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY);
    // required by storage policy
    public static final String S3_ROOT_PATH = "AWS_ROOT_PATH";
    public static final String S3_BUCKET = "AWS_BUCKET";

    // optional
    public static final String USE_PATH_STYLE = "use_path_style";
    public static final String S3_MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
    public static final String S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
    public static final String S3_CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
    public static final String DEFAULT_S3_MAX_CONNECTIONS = "50";
    public static final String DEFAULT_S3_REQUEST_TIMEOUT_MS = "3000";
    public static final String DEFAULT_S3_CONNECTION_TIMEOUT_MS = "1000";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    @SerializedName(value = "referenceSet")
    private Map<String, ReferenceType> references;

    public boolean addReference(String referenceName, ReferenceType type) throws AnalysisException {
        if (type == ReferenceType.POLICY) {
            if (!properties.containsKey(S3_ROOT_PATH)) {
                throw new AnalysisException(String.format("Missing [%s] in '%s' resource", S3_ROOT_PATH, name));
            }
            if (!properties.containsKey(S3_BUCKET)) {
                throw new AnalysisException(String.format("Missing [%s] in '%s' resource", S3_BUCKET, name));
            }
        }
        if (references.put(referenceName, type) == null) {
            // log set
            Env.getCurrentEnv().getEditLog().logAlterResource(this);
            LOG.info("Reference(type={}, name={}) is added to s3 resource, current set: {}",
                    type, referenceName, references);
            return true;
        }
        return false;
    }

    public S3Resource(String name) {
        this(name, Maps.newHashMap(), Maps.newHashMap());
    }

    public S3Resource(String name, Map<String, String> properties, Map<String, ReferenceType> policySet) {
        super(name, ResourceType.S3);
        this.properties = properties;
        this.references = policySet;
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
        checkRequiredProperty(S3_ACCESS_KEY);
        checkRequiredProperty(S3_SECRET_KEY);
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
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3_ENDPOINT, S3_REGION, S3_ROOT_PATH, S3_BUCKET);
            Optional<String> any = cantChangeProperties.stream().filter(properties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        // modify properties
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        notifyUpdate();
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    public void dropResource() throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            throw new DdlException("S3 resource used by policy, can't drop it.");
        }
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
        if (references.containsValue(ReferenceType.POLICY)) {
            SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
            AgentBatchTask batchTask = new AgentBatchTask();

            Map<String, String> copiedProperties = getCopiedProperties();

            for (Long beId : systemInfoService.getBackendIds(true)) {
                this.references.forEach(
                        (policy, type) -> {
                            if (type == ReferenceType.POLICY) {
                                List<Policy> policiesByType = Env.getCurrentEnv().getPolicyMgr()
                                        .getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                                Optional<Policy> findPolicy = policiesByType.stream()
                                        .filter(p -> p.getType() == PolicyTypeEnum.STORAGE
                                                && policy.equals(p.getPolicyName()))
                                        .findAny();
                                LOG.info("find policy in {} ", policiesByType);
                                if (!findPolicy.isPresent()) {
                                    return;
                                }
                                // add policy's coolDown ttl、coolDown data、policy name to map
                                Map<String, String> tmpMap = Maps.newHashMap(copiedProperties);
                                StoragePolicy used = (StoragePolicy) findPolicy.get();
                                tmpMap.put(StoragePolicy.COOLDOWN_DATETIME,
                                        String.valueOf(used.getCooldownTimestampMs()));

                                final String[] cooldownTtl = {"-1"};
                                Optional.ofNullable(used.getCooldownTtl())
                                        .ifPresent(date -> cooldownTtl[0] = used.getCooldownTtl());
                                tmpMap.put(StoragePolicy.COOLDOWN_TTL, cooldownTtl[0]);

                                tmpMap.put(StoragePolicy.MD5_CHECKSUM, used.getMd5Checksum());

                                NotifyUpdateStoragePolicyTask modifyS3ResourcePropertiesTask =
                                        new NotifyUpdateStoragePolicyTask(beId, used.getPolicyName(), tmpMap);
                                LOG.info("notify be: {}, policy name: {}, "
                                                + "properties: {} to modify S3 resource batch task.",
                                        beId, used.getPolicyName(), tmpMap);
                                batchTask.addTask(modifyS3ResourcePropertiesTask);
                            }
                        }
                );
            }
            AgentTaskExecutor.submit(batchTask);
        }
    }
}

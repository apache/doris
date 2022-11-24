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

package org.apache.doris.policy;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.NotifyUpdateStoragePolicyTask;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Save policy for storage migration.
 **/
@Data
public class StoragePolicy extends Policy {
    public static final String DEFAULT_STORAGE_POLICY_NAME = "default_storage_policy";

    public static boolean checkDefaultStoragePolicyValid(final String storagePolicyName, Optional<Policy> defaultPolicy)
            throws DdlException {
        if (!defaultPolicy.isPresent()) {
            return false;
        }

        if (storagePolicyName.equalsIgnoreCase(DEFAULT_STORAGE_POLICY_NAME) && (
                ((StoragePolicy) defaultPolicy.get()).getStorageResource() == null)) {
            throw new DdlException("Use default storage policy, but not give s3 info,"
                    + " please use alter resource to add default storage policy S3 info.");
        }
        return true;
    }

    public static final ShowResultSetMetaData STORAGE_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("StorageResource", ScalarType.createVarchar(20)))
                .addColumn(new Column("CooldownDatetime", ScalarType.createVarchar(20)))
                .addColumn(new Column("CooldownTtl", ScalarType.createVarchar(20)))
                .addColumn(new Column("properties", ScalarType.createVarchar(65535)))
                .build();

    private static final Logger LOG = LogManager.getLogger(StoragePolicy.class);
    // required
    public static final String STORAGE_RESOURCE = "storage_resource";
    // optional
    public static final String COOLDOWN_DATETIME = "cooldown_datetime";
    public static final String COOLDOWN_TTL = "cooldown_ttl";

    // for ttl format
    private static final String TTL_WEEK = "week";
    private static final String TTL_DAY = "day";
    private static final String TTL_DAY_SIMPLE = "d";
    private static final String TTL_HOUR = "hour";
    private static final String TTL_HOUR_SIMPLE = "h";
    private static final long ONE_HOUR_MS = 3600 * 1000;
    private static final long ONE_DAY_MS = 24 * ONE_HOUR_MS;
    private static final long ONE_WEEK_MS = 7 * ONE_DAY_MS;

    public static final String MD5_CHECKSUM = "md5_checksum";
    @SerializedName(value = "md5Checksum")
    private String md5Checksum = null;

    @SerializedName(value = "storageResource")
    private String storageResource = null;

    @SerializedName(value = "cooldownTimestampMs")
    private long cooldownTimestampMs = -1;

    @SerializedName(value = "cooldownTtl")
    private String cooldownTtl = null;

    @SerializedName(value = "cooldownTtlMs")
    private long cooldownTtlMs = 0;

    private Map<String, String> props;

    public StoragePolicy() {
        super(PolicyTypeEnum.STORAGE);
    }

    /**
     * Policy for Storage Migration.
     *
     * @param policyId policy id
     * @param policyName policy name
     * @param storageResource resource name for storage
     * @param cooldownTimestampMs cool down time
     * @param cooldownTtl cool down time cost after partition is created
     * @param cooldownTtlMs seconds for cooldownTtl
     */
    public StoragePolicy(long policyId, final String policyName, final String storageResource,
            final long cooldownTimestampMs, final String cooldownTtl, long cooldownTtlMs) {
        super(policyId, PolicyTypeEnum.STORAGE, policyName);
        this.storageResource = storageResource;
        this.cooldownTimestampMs = cooldownTimestampMs;
        this.cooldownTtl = cooldownTtl;
        this.cooldownTtlMs = cooldownTtlMs;
    }

    /**
     * Policy for Storage Migration.
     *
     * @param policyId policy id
     * @param policyName policy name
     */
    public StoragePolicy(long policyId, final String policyName) {
        super(policyId, PolicyTypeEnum.STORAGE, policyName);
    }

    public static StoragePolicy ofCheck(String policyName) {
        StoragePolicy storagePolicy = new StoragePolicy();
        storagePolicy.policyName = policyName;
        return storagePolicy;
    }

    /**
     * Init props for storage policy.
     *
     * @param props properties for storage policy
     */
    public void init(final Map<String, String> props, boolean ifNotExists) throws AnalysisException {
        if (props == null) {
            throw new AnalysisException("properties config is required");
        }
        checkRequiredProperty(props, STORAGE_RESOURCE);
        this.storageResource = props.get(STORAGE_RESOURCE);
        boolean hasCooldownDatetime = false;
        boolean hasCooldownTtl = false;
        if (props.containsKey(COOLDOWN_DATETIME)) {
            hasCooldownDatetime = true;
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                this.cooldownTimestampMs = df.parse(props.get(COOLDOWN_DATETIME)).getTime();
            } catch (ParseException e) {
                throw new AnalysisException(String.format("cooldown_datetime format error: %s",
                                            props.get(COOLDOWN_DATETIME)), e);
            }
        }
        if (props.containsKey(COOLDOWN_TTL)) {
            hasCooldownTtl = true;
            // second
            this.cooldownTtl = String.valueOf(getMsByCooldownTtl(props.get(COOLDOWN_TTL)) / 1000);
        }
        if (hasCooldownDatetime && hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " and " + COOLDOWN_TTL + " can't be set together.");
        }
        if (!hasCooldownDatetime && !hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " or " + COOLDOWN_TTL + " must be set");
        }

        // no set ttl use -1
        if (!hasCooldownTtl) {
            this.cooldownTtl = "-1";
        }

        Resource r = checkIsS3ResourceAndExist(this.storageResource);
        if (!((S3Resource) r).policyAddToSet(super.getPolicyName()) && !ifNotExists) {
            throw new AnalysisException("this policy has been added to s3 resource once, policy has been created.");
        }
        this.md5Checksum = calcPropertiesMd5();
    }

    private static Resource checkIsS3ResourceAndExist(final String storageResource) throws AnalysisException {
        // check storage_resource type is S3, current just support S3
        Resource resource =
                Optional.ofNullable(Env.getCurrentEnv().getResourceMgr().getResource(storageResource))
                    .orElseThrow(() -> new AnalysisException("storage resource doesn't exist: " + storageResource));

        if (resource.getType() != Resource.ResourceType.S3) {
            throw new AnalysisException("current storage policy just support resource type S3");
        }
        return resource;
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        final String[] props = {""};
        if (Env.getCurrentEnv().getResourceMgr().containsResource(this.storageResource)) {
            props[0] = Env.getCurrentEnv().getResourceMgr().getResource(this.storageResource).toString();
        }
        if (!props[0].equals("")) {
            // s3_secret_key => ******
            S3Resource s3Resource = GsonUtils.GSON.fromJson(props[0], S3Resource.class);
            Optional.ofNullable(s3Resource).ifPresent(s3 -> {
                Map<String, String> copyMap = s3.getCopiedProperties();
                copyMap.put(S3Resource.S3_SECRET_KEY, "******");
                props[0] = GsonUtils.GSON.toJson(copyMap);
            });
        }
        if (cooldownTimestampMs == -1) {
            return Lists.newArrayList(this.policyName, this.type.name(), this.storageResource, "-1", this.cooldownTtl,
                    props[0]);
        }
        return Lists.newArrayList(this.policyName, this.type.name(), this.storageResource,
                TimeUtils.longToTimeString(this.cooldownTimestampMs), this.cooldownTtl, props[0]);
    }

    @Override
    public void gsonPostProcess() throws IOException {}

    @Override
    public StoragePolicy clone() {
        return new StoragePolicy(this.policyId, this.policyName, this.storageResource, this.cooldownTimestampMs,
                this.cooldownTtl, this.cooldownTtlMs);
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        if (!(checkedPolicyCondition instanceof StoragePolicy)) {
            return false;
        }
        StoragePolicy storagePolicy = (StoragePolicy) checkedPolicyCondition;
        return (storagePolicy.getStorageResource() == null
                        || storagePolicy.getStorageResource().equals(this.storageResource))
                && checkMatched(storagePolicy.getType(), storagePolicy.getPolicyName());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog checkedDropCondition) {
        return checkMatched(checkedDropCondition.getType(), checkedDropCondition.getPolicyName());
    }

    /**
     * check required key in properties.
     *
     * @param props properties for storage policy
     * @param propertyKey key for property
     * @throws AnalysisException exception for properties error
     */
    private void checkRequiredProperty(final Map<String, String> props, String propertyKey) throws AnalysisException {
        String value = props.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new AnalysisException("Missing [" + propertyKey + "] in properties.");
        }
    }

    @Override
    public boolean isInvalid() {
        return false;
    }

    /**
     * Get milliseconds by cooldownTtl, 1week=604800000 1day=1d=86400000, 1hour=1h=3600000
     * @param cooldownTtl cooldown ttl
     * @return millisecond for cooldownTtl
     */
    private static long getMsByCooldownTtl(String cooldownTtl) throws AnalysisException {
        cooldownTtl = cooldownTtl.replace(TTL_DAY, TTL_DAY_SIMPLE).replace(TTL_HOUR, TTL_HOUR_SIMPLE);
        long cooldownTtlMs = 0;
        try {
            if (cooldownTtl.endsWith(TTL_DAY_SIMPLE)) {
                cooldownTtlMs = Long.parseLong(cooldownTtl.replace(TTL_DAY_SIMPLE, "").trim()) * ONE_DAY_MS;
            } else if (cooldownTtl.endsWith(TTL_HOUR_SIMPLE)) {
                cooldownTtlMs = Long.parseLong(cooldownTtl.replace(TTL_HOUR_SIMPLE, "").trim()) * ONE_HOUR_MS;
            } else if (cooldownTtl.endsWith(TTL_WEEK)) {
                cooldownTtlMs = Long.parseLong(cooldownTtl.replace(TTL_WEEK, "").trim()) * ONE_WEEK_MS;
            } else {
                cooldownTtlMs = Long.parseLong(cooldownTtl.trim()) * 1000;
            }
        } catch (NumberFormatException e) {
            LOG.error("getSecByCooldownTtl failed.", e);
            throw new AnalysisException("getSecByCooldownTtl failed.", e);
        }
        if (cooldownTtlMs < 0) {
            LOG.error("cooldownTtl can't be less than 0");
            throw new AnalysisException("cooldownTtl can't be less than 0");
        }
        return cooldownTtlMs;
    }

    // be use this md5Sum to determine whether storage policy has been changed.
    // if md5Sum not eq previous value, be change its storage policy.
    private String calcPropertiesMd5() {
        List<String> calcKey = Arrays.asList(COOLDOWN_DATETIME, COOLDOWN_TTL, S3Resource.S3_MAX_CONNECTIONS,
                S3Resource.S3_REQUEST_TIMEOUT_MS, S3Resource.S3_CONNECTION_TIMEOUT_MS, S3Resource.S3_ACCESS_KEY,
                S3Resource.S3_SECRET_KEY);
        Map<String, String> copiedStoragePolicyProperties = Env.getCurrentEnv().getResourceMgr()
                .getResource(this.storageResource).getCopiedProperties();

        copiedStoragePolicyProperties.put(COOLDOWN_DATETIME, String.valueOf(this.cooldownTimestampMs));
        copiedStoragePolicyProperties.put(COOLDOWN_TTL, this.cooldownTtl);

        LOG.info("calcPropertiesMd5 map {}", copiedStoragePolicyProperties);

        return DigestUtils.md5Hex(calcKey.stream()
                .map(iter -> "(" + iter + ":" + copiedStoragePolicyProperties.get(iter) + ")")
                .reduce("", String::concat));
    }

    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        // check properties
        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        copiedProperties.remove(STORAGE_RESOURCE);
        copiedProperties.remove(COOLDOWN_DATETIME);
        copiedProperties.remove(COOLDOWN_TTL);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown Storage policy properties: " + copiedProperties);
        }
    }

    public void modifyProperties(Map<String, String> properties) throws DdlException, AnalysisException {
        Optional.ofNullable(properties.get(COOLDOWN_TTL)).ifPresent(this::setCooldownTtl);
        Optional.ofNullable(properties.get(COOLDOWN_DATETIME)).ifPresent(date -> {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                this.cooldownTimestampMs = df.parse(properties.get(COOLDOWN_DATETIME)).getTime();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });

        if (policyName.equalsIgnoreCase(DEFAULT_STORAGE_POLICY_NAME) && storageResource == null) {
            // here first time set S3 resource to default storage policy.
            String alterStorageResource = Optional.ofNullable(properties.get(STORAGE_RESOURCE)).orElseThrow(
                    () -> new DdlException("first time set default storage policy, but not give storageResource"));
            // check alterStorageResource resource exist.
            checkIsS3ResourceAndExist(alterStorageResource);
            storageResource = alterStorageResource;
        }

        md5Checksum = calcPropertiesMd5();
        notifyUpdate();
    }

    private void notifyUpdate() {
        SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
        AgentBatchTask batchTask = new AgentBatchTask();

        for (Long beId : systemInfoService.getBackendIds(true)) {
            Map<String, String> copiedProperties = Env.getCurrentEnv().getResourceMgr().getResource(storageResource)
                    .getCopiedProperties();

            Map<String, String> tmpMap = Maps.newHashMap(copiedProperties);

            tmpMap.put(COOLDOWN_DATETIME, String.valueOf(this.cooldownTimestampMs));

            Optional.ofNullable(this.getCooldownTtl()).ifPresent(date -> {
                tmpMap.put(COOLDOWN_TTL, this.getCooldownTtl());
            });
            tmpMap.put(MD5_CHECKSUM, this.getMd5Checksum());
            NotifyUpdateStoragePolicyTask notifyUpdateStoragePolicyTask = new NotifyUpdateStoragePolicyTask(beId,
                    getPolicyName(), tmpMap);
            batchTask.addTask(notifyUpdateStoragePolicyTask);
            LOG.info("update policy info to be: {}, policy name: {}, "
                    + "properties: {} to modify S3 resource batch task.", beId, getPolicyName(), tmpMap);
        }

        AgentTaskExecutor.submit(batchTask);
    }

    public static StoragePolicy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StoragePolicy.class);
    }
}

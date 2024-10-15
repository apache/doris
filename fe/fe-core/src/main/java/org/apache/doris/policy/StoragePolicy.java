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
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ReferenceType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
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
                .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                .addColumn(new Column("StorageResource", ScalarType.createVarchar(20)))
                .addColumn(new Column("CooldownDatetime", ScalarType.createVarchar(20)))
                .addColumn(new Column("CooldownTtl", ScalarType.createVarchar(20)))
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
    private static final long ONE_HOUR_S = 3600;
    private static final long ONE_DAY_S = 24 * ONE_HOUR_S;
    private static final long ONE_WEEK_S = 7 * ONE_DAY_S;

    @SerializedName(value = "storageResource")
    private String storageResource = null;

    @SerializedName(value = "cooldownTimestampMs")
    private long cooldownTimestampMs = -1;

    // time unit: seconds
    @SerializedName(value = "cooldownTtl")
    private long cooldownTtl = -1;

    // for Gson fromJson
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
     * @param cooldownTtl seconds for cooldownTtl
     */
    public StoragePolicy(long policyId, final String policyName, final String storageResource,
            final long cooldownTimestampMs, long cooldownTtl) {
        super(policyId, PolicyTypeEnum.STORAGE, policyName);
        this.storageResource = storageResource;
        this.cooldownTimestampMs = cooldownTimestampMs;
        this.cooldownTtl = cooldownTtl;
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
        boolean hasCooldownDatetime = props.containsKey(COOLDOWN_DATETIME);
        boolean hasCooldownTtl = props.containsKey(COOLDOWN_TTL);

        if (hasCooldownDatetime && hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " and " + COOLDOWN_TTL + " can't be set together.");
        }
        if (!hasCooldownDatetime && !hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " or " + COOLDOWN_TTL + " must be set");
        }
        if (hasCooldownDatetime) {
            try {
                this.cooldownTimestampMs = LocalDateTime
                        .parse(props.get(COOLDOWN_DATETIME), TimeUtils.getDatetimeFormatWithTimeZone())
                        .atZone(TimeUtils.getDorisZoneId()).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new AnalysisException(String.format("cooldown_datetime format error: %s",
                        props.get(COOLDOWN_DATETIME)), e);
            }
            // ttl would be set as -1 when using datetime
            this.cooldownTtl = -1;
        }
        if (hasCooldownTtl) {
            // second
            // this.cooldownTtlMs = (getMsByCooldownTtl(props.get(COOLDOWN_TTL)) / 1000);
            this.cooldownTtl = getSecondsByCooldownTtl(props.get(COOLDOWN_TTL));
        }

        checkResourceIsExist(this.storageResource);
        if (!addResourceReference() && !ifNotExists) {
            throw new AnalysisException("this policy has been added to s3 or hdfs resource, policy has been created.");
        }
    }

    private static Resource checkResourceIsExist(final String storageResource) throws AnalysisException {
        Resource resource =
                Optional.ofNullable(Env.getCurrentEnv().getResourceMgr().getResource(storageResource))
                    .orElseThrow(() -> new AnalysisException("storage resource doesn't exist: " + storageResource));

        Map<String, String> properties = resource.getCopiedProperties();
        switch (resource.getType()) {
            case S3:
                if (!properties.containsKey(S3Properties.ROOT_PATH)) {
                    throw new AnalysisException(String.format(
                        "Missing [%s] in '%s' resource", S3Properties.ROOT_PATH, storageResource));
                }
                if (!properties.containsKey(S3Properties.BUCKET)) {
                    throw new AnalysisException(String.format(
                        "Missing [%s] in '%s' resource", S3Properties.BUCKET, storageResource));
                }
                break;
            case HDFS:
                if (!properties.containsKey(HdfsResource.HADOOP_FS_NAME)) {
                    throw new AnalysisException(String.format(
                        "Missing [%s] in '%s' resource", HdfsResource.HADOOP_FS_NAME, storageResource));
                }
                break;
            default:
                throw new AnalysisException(
                    "current storage policy just support resource type S3_COOLDOWN or HDFS_COOLDOWN");
        }
        return resource;
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        readLock();
        try {
            if (cooldownTimestampMs == -1) {
                return Lists.newArrayList(this.policyName, String.valueOf(this.id), String.valueOf(this.version),
                        this.type.name(), this.storageResource, "-1", String.valueOf(this.cooldownTtl));
            }
            return Lists.newArrayList(this.policyName, String.valueOf(this.id), String.valueOf(this.version),
                    this.type.name(), this.storageResource, TimeUtils.longToTimeString(this.cooldownTimestampMs),
                    String.valueOf(this.cooldownTtl));
        } finally {
            readUnlock();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {}

    @Override
    public StoragePolicy clone() {
        return new StoragePolicy(this.id, this.policyName, this.storageResource, this.cooldownTimestampMs,
                this.cooldownTtl);
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
    public static long getSecondsByCooldownTtl(String cooldownTtl) throws AnalysisException {
        cooldownTtl = cooldownTtl.replace(TTL_DAY, TTL_DAY_SIMPLE).replace(TTL_HOUR, TTL_HOUR_SIMPLE);
        long cooldownTtlSeconds = 0;
        try {
            if (cooldownTtl.endsWith(TTL_DAY_SIMPLE)) {
                cooldownTtlSeconds = Long.parseLong(cooldownTtl.replace(TTL_DAY_SIMPLE, "").trim()) * ONE_DAY_S;
            } else if (cooldownTtl.endsWith(TTL_HOUR_SIMPLE)) {
                cooldownTtlSeconds = Long.parseLong(cooldownTtl.replace(TTL_HOUR_SIMPLE, "").trim()) * ONE_HOUR_S;
            } else if (cooldownTtl.endsWith(TTL_WEEK)) {
                cooldownTtlSeconds = Long.parseLong(cooldownTtl.replace(TTL_WEEK, "").trim()) * ONE_WEEK_S;
            } else {
                cooldownTtlSeconds = Long.parseLong(cooldownTtl.trim());
            }
        } catch (NumberFormatException e) {
            LOG.error("getSecByCooldownTtl failed.", e);
            throw new AnalysisException("getSecByCooldownTtl failed.", e);
        }
        if (cooldownTtlSeconds < 0) {
            LOG.error("cooldownTtl can't be less than 0");
            throw new AnalysisException("cooldownTtl can't be less than 0");
        }
        return cooldownTtlSeconds;
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
        this.toString();
        // check cooldown date time and ttl.
        long cooldownTtlMs = this.cooldownTtl;
        long cooldownTimestampMs = this.cooldownTimestampMs;
        if (properties.containsKey(COOLDOWN_TTL)) {
            if (properties.get(COOLDOWN_TTL).isEmpty()) {
                cooldownTtlMs = -1;
            } else {
                cooldownTtlMs = getSecondsByCooldownTtl(properties.get(COOLDOWN_TTL));
            }
        }
        if (properties.containsKey(COOLDOWN_DATETIME)) {
            if (properties.get(COOLDOWN_DATETIME).isEmpty()) {
                cooldownTimestampMs = -1;
            } else {
                try {
                    cooldownTimestampMs = LocalDateTime.parse(properties.get(COOLDOWN_DATETIME),
                            TimeUtils.getDatetimeFormatWithTimeZone()).atZone(TimeUtils.getDorisZoneId()).toInstant()
                            .toEpochMilli();
                } catch (DateTimeParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (cooldownTtlMs > 0 && cooldownTimestampMs > 0) {
            throw new AnalysisException(COOLDOWN_DATETIME + " and " + COOLDOWN_TTL + " can't be set together.");
        }
        if (cooldownTtlMs <= 0 && cooldownTimestampMs <= 0) {
            throw new AnalysisException(COOLDOWN_DATETIME + " or " + COOLDOWN_TTL + " must be set");
        }

        String storageResource = properties.get(STORAGE_RESOURCE);
        if (storageResource != null) {
            checkResourceIsExist(storageResource);
        }
        if (this.policyName.equalsIgnoreCase(DEFAULT_STORAGE_POLICY_NAME) && this.storageResource == null
                && storageResource == null) {
            throw new DdlException("first time set default storage policy, but not give storageResource");
        }
        // modify properties
        writeLock();
        this.cooldownTtl = cooldownTtlMs;
        this.cooldownTimestampMs = cooldownTimestampMs;
        if (storageResource != null) {
            this.storageResource = storageResource;
        }
        ++version;
        writeUnlock();
    }

    public boolean addResourceReference() {
        if (storageResource != null) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(storageResource);
            if (resource != null) {
                return resource.addReference(policyName, ReferenceType.POLICY);
            }
        }
        return false;
    }

    public boolean removeResourceReference() {
        if (storageResource != null) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(storageResource);
            if (resource != null) {
                return resource.removeReference(policyName, ReferenceType.POLICY);
            }
        }
        return false;
    }
}

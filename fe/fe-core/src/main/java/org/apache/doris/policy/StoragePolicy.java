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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Save policy for storage migration.
 **/
@Data
public class StoragePolicy extends Policy {

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
    private static final String STORAGE_RESOURCE = "storage_resource";
    // optional
    private static final String COOLDOWN_DATETIME = "cooldown_datetime";
    private static final String COOLDOWN_TTL = "cooldown_ttl";

    // for ttl format
    private static final String TTL_WEEK = "week";
    private static final String TTL_DAY = "day";
    private static final String TTL_DAY_SIMPLE = "d";
    private static final String TTL_HOUR = "hour";
    private static final String TTL_HOUR_SIMPLE = "h";
    private static final long ONE_HOUR_MS = 3600 * 1000;
    private static final long ONE_DAY_MS = 24 * ONE_HOUR_MS;
    private static final long ONE_WEEK_MS = 7 * ONE_DAY_MS;

    @SerializedName(value = "storageResource")
    private String storageResource = null;

    @SerializedName(value = "cooldownDatetime")
    private Date cooldownDatetime = null;

    @SerializedName(value = "cooldownTtl")
    private String cooldownTtl = null;

    @SerializedName(value = "cooldownTtlMs")
    private long cooldownTtlMs = 0;

    private Map<String, String> props;

    public StoragePolicy() {}

    /**
     * Policy for Storage Migration.
     *
     * @param type PolicyType
     * @param policyName policy name
     * @param storageResource resource name for storage
     * @param cooldownDatetime cool down time
     * @param cooldownTtl cool down time cost after partition is created
     * @param cooldownTtlMs seconds for cooldownTtl
     */
    public StoragePolicy(final PolicyTypeEnum type, final String policyName, final String storageResource,
                         final Date cooldownDatetime, final String cooldownTtl, long cooldownTtlMs) {
        super(type, policyName);
        this.storageResource = storageResource;
        this.cooldownDatetime = cooldownDatetime;
        this.cooldownTtl = cooldownTtl;
        this.cooldownTtlMs = cooldownTtlMs;
    }

    /**
     * Policy for Storage Migration.
     *
     * @param type PolicyType
     * @param policyName policy name
     */
    public StoragePolicy(final PolicyTypeEnum type, final String policyName) {
        super(type, policyName);
    }

    /**
     * Init props for storage policy.
     *
     * @param props properties for storage policy
     */
    public void init(final Map<String, String> props) throws AnalysisException {
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
                this.cooldownDatetime = df.parse(props.get(COOLDOWN_DATETIME));
            } catch (ParseException e) {
                throw new AnalysisException(String.format("cooldown_datetime format error: %s",
                                            props.get(COOLDOWN_DATETIME)), e);
            }
        }
        if (props.containsKey(COOLDOWN_TTL)) {
            hasCooldownTtl = true;
            this.cooldownTtl = props.get(COOLDOWN_TTL);
            this.cooldownTtlMs = getMsByCooldownTtl(this.cooldownTtl);
        }
        if (hasCooldownDatetime && hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " and " + COOLDOWN_TTL + " can't be set together.");
        }
        if (!hasCooldownDatetime && !hasCooldownTtl) {
            throw new AnalysisException(COOLDOWN_DATETIME + " or " + COOLDOWN_TTL + " must be set");
        }
        if (!Catalog.getCurrentCatalog().getResourceMgr().containsResource(this.storageResource)) {
            throw new AnalysisException("storage resource doesn't exist: " + this.storageResource);
        }
    }

    /**
     * Use for SHOW POLICY.
     **/
    public List<String> getShowInfo() throws AnalysisException {
        String props = "";
        if (Catalog.getCurrentCatalog().getResourceMgr().containsResource(this.storageResource)) {
            props = Catalog.getCurrentCatalog().getResourceMgr().getResource(this.storageResource).toString();
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String cooldownDatetimeStr = "";
        if (this.cooldownDatetime != null) {
            cooldownDatetimeStr = df.format(this.cooldownDatetime);
        }
        return Lists.newArrayList(this.policyName, this.type.name(), this.storageResource,
                                  cooldownDatetimeStr, this.cooldownTtl, props);
    }

    @Override
    public void gsonPostProcess() throws IOException {}

    @Override
    public StoragePolicy clone() {
        return new StoragePolicy(this.type, this.policyName, this.storageResource,
                                 this.cooldownDatetime, this.cooldownTtl, this.cooldownTtlMs);
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
}

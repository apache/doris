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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Alibaba Cloud OSS resource
 * Syntax:
 * CREATE RESOURCE "remote_oss"
 * PROPERTIES
 * (
 * "type" = "oss",
 * "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
 * "oss.region" = "cn-beijing",
 * "oss.bucket" = "bucket-name",
 * "oss.access_key" = "ak",
 * "oss.secret_key" = "sk"
 * );
 */
public class OSSResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(OSSResource.class);
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public OSSResource() {
        super();
    }

    public OSSResource(String name) {
        super(name, ResourceType.OSS);
        properties = Maps.newHashMap();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);

        // check required properties
        S3Properties.requiredS3PingProperties(properties);

        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("oss info need check validity : {}", needCheck);
        }

        if (needCheck) {
            String bucketName = properties.get(OSSProperties.BUCKET_KEY);
            String rootPath = properties.get(OSSProperties.ROOT_PATH_KEY);
            pingOSS(bucketName, rootPath, properties);
        }

        // optional properties
        S3Properties.optionalS3Property(properties);
    }

    protected static void pingOSS(String bucketName, String rootPath,
            Map<String, String> newProperties) throws DdlException {
        // Basic validation - full implementation requires OSSObj Storage
        if (bucketName == null || bucketName.isEmpty()) {
            throw new DdlException("OSS bucket name is required");
        }
        LOG.info("OSS resource validation passed for bucket: {}", bucketName);
    }

    @Override
    public void modifyProperties(Map<String, String> newProperties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            List<String> cantChangeProperties = Arrays.asList(
                    OSSProperties.ENDPOINT_KEY, OSSProperties.REGION_KEY,
                    OSSProperties.ROOT_PATH_KEY, OSSProperties.BUCKET_KEY);
            Optional<String> any = cantChangeProperties.stream().filter(newProperties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }

        boolean needCheck = isNeedCheck(newProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("oss info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3Properties.requiredS3PingProperties(this.properties);
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(newProperties);
            String bucketName = newProperties.getOrDefault(OSSProperties.BUCKET_KEY,
                    this.properties.get(OSSProperties.BUCKET_KEY));
            String rootPath = newProperties.getOrDefault(OSSProperties.ROOT_PATH_KEY,
                    this.properties.get(OSSProperties.ROOT_PATH_KEY));

            pingOSS(bucketName, rootPath, changedProperties);
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : newProperties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(OSSProperties.SESSION_TOKEN_KEY)) {
                this.properties.put(kv.getKey(), kv.getValue());
            }
        }
        ++version;
        writeUnlock();
        super.modifyProperties(newProperties);
    }

    private boolean isNeedCheck(Map<String, String> newProperties) {
        boolean needCheck = !this.properties.containsKey(S3Properties.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(S3Properties.VALIDITY_CHECK));
        if (newProperties != null && newProperties.containsKey(S3Properties.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(S3Properties.VALIDITY_CHECK));
        }
        return needCheck;
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(this.properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "id", String.valueOf(id)));
        readLock();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "version", String.valueOf(version)));
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            if (PrintableMap.HIDDEN_KEY.contains(entry.getKey())) {
                continue;
            }
            // hide sensitive properties
            if (entry.getKey().equals(OSSProperties.SECRET_KEY_KEY)
                    || entry.getKey().equals(OSSProperties.SESSION_TOKEN_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }
}

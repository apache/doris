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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Alibaba Cloud OSS resource. Accepts both {@code oss.*} and {@code s3.*} property prefixes;
 * oss.* keys are normalised to s3.* equivalents so downstream logic uses a single key space.
 * When role_arn is set without AK/SK the connectivity check is skipped -- the BE resolves
 * credentials at query time.
 * <pre>
 * CREATE RESOURCE "remote_oss"
 * PROPERTIES
 * (
 *   "type"         = "s3",
 *   "s3.endpoint"  = "oss-cn-beijing.aliyuncs.com",
 *   "s3.region"    = "cn-beijing",
 *   "s3.bucket"    = "my-bucket",
 *   "s3.root.path" = "/path/to/root",
 *   "s3.access_key" = "ak",
 *   "s3.secret_key" = "sk"
 * );
 * </pre>
 */
public class OSSResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(OSSResource.class);
    private Map<String, String> properties = Maps.newHashMap();

    public OSSResource() {
        super();
    }

    public OSSResource(String name) {
        super(name, ResourceType.OSS);
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);
        normalizeOssToS3Keys(this.properties);
        S3Properties.requiredS3PingProperties(this.properties);
        boolean needCheck = isNeedCheck(this.properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("oss info need check validity : {}", needCheck);
        }
        String pingEndpoint = this.properties.get(S3Properties.ENDPOINT);
        if (!pingEndpoint.startsWith("http://") && !pingEndpoint.startsWith("https://")) {
            pingEndpoint = "https://" + pingEndpoint;
            this.properties.put(S3Properties.ENDPOINT, pingEndpoint);
            this.properties.put(S3Properties.Env.ENDPOINT, pingEndpoint);
        }
        if (needCheck) {
            pingOSS(this.properties.get(S3Properties.BUCKET),
                    this.properties.get(S3Properties.ROOT_PATH), this.properties);
        }
        S3Properties.optionalS3Property(this.properties);
    }

    protected static void pingOSS(String bucketName, String rootPath,
            Map<String, String> props) throws DdlException {
        S3Resource.pingS3(bucketName, rootPath, props);
    }

    @Override
    public void modifyProperties(Map<String, String> newProperties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            List<String> cantChangeProperties = Arrays.asList(
                    OSSProperties.ENDPOINT_KEY, OSSProperties.REGION_KEY,
                    OSSProperties.ROOT_PATH_KEY, OSSProperties.BUCKET_KEY,
                    S3Properties.ENDPOINT, S3Properties.REGION,
                    S3Properties.ROOT_PATH, S3Properties.BUCKET,
                    S3Properties.Env.ENDPOINT, S3Properties.Env.REGION,
                    S3Properties.Env.ROOT_PATH, S3Properties.Env.BUCKET);
            Optional<String> any = cantChangeProperties.stream()
                    .filter(newProperties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        normalizeOssToS3Keys(newProperties);
        boolean needCheck = isNeedCheck(newProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("oss info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3Properties.requiredS3PingProperties(this.properties);
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(newProperties);
            String bucket = newProperties.containsKey(S3Properties.BUCKET)
                    ? newProperties.get(S3Properties.BUCKET)
                    : this.properties.get(S3Properties.BUCKET);
            if (bucket == null) {
                throw new DdlException("Missing required property: " + S3Properties.BUCKET);
            }
            pingOSS(bucket,
                    newProperties.getOrDefault(S3Properties.ROOT_PATH,
                            this.properties.get(S3Properties.ROOT_PATH)),
                    changedProperties);
        }
        writeLock();
        for (Map.Entry<String, String> kv : newProperties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(S3Properties.SESSION_TOKEN)
                    || kv.getKey().equals(OSSProperties.SESSION_TOKEN_KEY)) {
                // replaceIfEffectiveValue skips empty strings; force-put here so that setting
                // session_token="" explicitly clears a previously stored temporary token.
                this.properties.put(kv.getKey(), kv.getValue());
            }
            if ((kv.getKey().equalsIgnoreCase(OSSProperties.ROLE_ARN_KEY)
                    || kv.getKey().equalsIgnoreCase(S3Properties.ROLE_ARN))
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                this.properties.remove(OSSProperties.ACCESS_KEY_KEY);
                this.properties.remove(OSSProperties.SECRET_KEY_KEY);
                this.properties.remove(S3Properties.ACCESS_KEY);
                this.properties.remove(S3Properties.Env.ACCESS_KEY);
                this.properties.remove(S3Properties.SECRET_KEY);
                this.properties.remove(S3Properties.Env.SECRET_KEY);
            }
            if ((kv.getKey().equalsIgnoreCase(OSSProperties.ACCESS_KEY_KEY)
                    || kv.getKey().equalsIgnoreCase(S3Properties.ACCESS_KEY))
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                this.properties.remove(OSSProperties.ROLE_ARN_KEY);
                this.properties.remove(OSSProperties.EXTERNAL_ID_KEY);
                this.properties.remove(S3Properties.ROLE_ARN);
                this.properties.remove(S3Properties.Env.ROLE_ARN);
                this.properties.remove(S3Properties.EXTERNAL_ID);
                this.properties.remove(S3Properties.Env.EXTERNAL_ID);
            }
        }
        ++version;
        writeUnlock();
        super.modifyProperties(newProperties);
    }

    // Skip check when role_arn is set without AK/SK (FE cannot call Alibaba Cloud STS).
    // When both role_arn and AK/SK are set, ping uses AK/SK; STS is resolved by BE at query time.
    private boolean isNeedCheck(Map<String, String> newProperties) {
        String roleArn = newProperties.getOrDefault(S3Properties.ROLE_ARN,
                newProperties.getOrDefault(OSSProperties.ROLE_ARN_KEY,
                        this.properties.getOrDefault(S3Properties.ROLE_ARN,
                                this.properties.getOrDefault(OSSProperties.ROLE_ARN_KEY, ""))));
        String accessKey = newProperties.getOrDefault(S3Properties.ACCESS_KEY,
                newProperties.getOrDefault(OSSProperties.ACCESS_KEY_KEY,
                        this.properties.getOrDefault(S3Properties.ACCESS_KEY,
                                this.properties.getOrDefault(OSSProperties.ACCESS_KEY_KEY, ""))));
        if (!roleArn.isEmpty() && accessKey.isEmpty()) {
            return false;
        }
        boolean needCheck = !this.properties.containsKey(S3Properties.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(S3Properties.VALIDITY_CHECK));
        if (newProperties != null && newProperties.containsKey(S3Properties.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(S3Properties.VALIDITY_CHECK));
        }
        return needCheck;
    }

    private static void normalizeOssToS3Keys(Map<String, String> props) {
        copyIfAbsent(props, OSSProperties.ENDPOINT_KEY,      S3Properties.ENDPOINT);
        copyIfAbsent(props, OSSProperties.REGION_KEY,        S3Properties.REGION);
        copyIfAbsent(props, OSSProperties.ACCESS_KEY_KEY,    S3Properties.ACCESS_KEY);
        copyIfAbsent(props, OSSProperties.SECRET_KEY_KEY,    S3Properties.SECRET_KEY);
        copyIfAbsent(props, OSSProperties.SESSION_TOKEN_KEY, S3Properties.SESSION_TOKEN);
        copyIfAbsent(props, OSSProperties.BUCKET_KEY,        S3Properties.BUCKET);
        copyIfAbsent(props, OSSProperties.ROOT_PATH_KEY,     S3Properties.ROOT_PATH);
        copyIfAbsent(props, OSSProperties.ROLE_ARN_KEY,      S3Properties.ROLE_ARN);
        copyIfAbsent(props, OSSProperties.EXTERNAL_ID_KEY,   S3Properties.EXTERNAL_ID);
    }

    private static void copyIfAbsent(Map<String, String> props, String srcKey, String dstKey) {
        if (!props.containsKey(dstKey) && props.containsKey(srcKey)) {
            props.put(dstKey, props.get(srcKey));
        }
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
            if (entry.getKey().equals(S3Properties.Env.SECRET_KEY)
                    || entry.getKey().equals(S3Properties.SECRET_KEY)
                    || entry.getKey().equals(OSSProperties.SECRET_KEY_KEY)
                    || entry.getKey().equals(S3Properties.Env.TOKEN)
                    || entry.getKey().equals(S3Properties.SESSION_TOKEN)
                    || entry.getKey().equals(OSSProperties.SESSION_TOKEN_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }
}

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

import org.apache.doris.backup.Status;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.ObjStorage;
import org.apache.doris.fs.obj.RemoteObjects;
import org.apache.doris.fs.obj.S3ObjStorage;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * S3 resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_s3"
 * PROPERTIES
 * (
 * "type" = "s3",
 * "AWS_ENDPOINT" = "bj",
 * "AWS_REGION" = "bj",
 * "AWS_ROOT_PATH" = "/path/to/root",
 * "AWS_ACCESS_KEY" = "bbb",
 * "AWS_SECRET_KEY" = "aaaa",
 * "AWS_MAX_CONNECTION" = "50",
 * "AWS_REQUEST_TIMEOUT_MS" = "3000",
 * "AWS_CONNECTION_TIMEOUT_MS" = "1000"
 * );
 * <p>
 * For AWS S3, BE need following properties:
 * 1. AWS_ACCESS_KEY: ak
 * 2. AWS_SECRET_KEY: sk
 * 3. AWS_ENDPOINT: s3.us-east-1.amazonaws.com
 * 4. AWS_REGION: us-east-1
 * And file path: s3://bucket_name/csv/taxi.csv
 */
public class S3Resource extends Resource {
    private static final Logger LOG = LogManager.getLogger(S3Resource.class);
    // Only aliases already supported by GCSProperties belong here. Resource/Vault options
    // without a GCS alias (bucket, root path, region, validity checking) keep their existing names.
    private static final Map<String, String> GCS_PROPERTY_ALIASES = ImmutableMap.<String, String>builder()
            .put(GCSProperties.ENDPOINT, S3Properties.ENDPOINT)
            .put("gs.access_key", S3Properties.ACCESS_KEY)
            .put("gs.secret_key", S3Properties.SECRET_KEY)
            .put("gs.session_token", S3Properties.SESSION_TOKEN)
            .put("gs.connection.maximum", S3Properties.MAX_CONNECTIONS)
            .put("gs.connection.request.timeout", S3Properties.REQUEST_TIMEOUT_MS)
            .put("gs.connection.timeout", S3Properties.CONNECTION_TIMEOUT_MS)
            .put("gs.use_path_style", S3Properties.USE_PATH_STYLE)
            .put("gs.force_parsing_by_standard_uri", "force_parsing_by_standard_uri")
            .build();

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public S3Resource() {
        super();
    }

    public S3Resource(String name) {
        super(name, ResourceType.S3);
        properties = Maps.newHashMap();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);
        normalizeProperties(this.properties, this.properties.get(StorageProperties.FS_PROVIDER_KEY));

        // check properties
        S3Properties.requiredS3PingProperties(properties);
        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }

        // the endpoint for ping need add uri scheme.
        String pingEndpoint = properties.get(S3Properties.ENDPOINT);
        if (!pingEndpoint.startsWith("http://") && !pingEndpoint.startsWith("https://")) {
            pingEndpoint = "http://" + properties.get(S3Properties.ENDPOINT);
            properties.put(S3Properties.ENDPOINT, pingEndpoint);
            properties.put(S3Properties.Env.ENDPOINT, pingEndpoint);
        }
        String region = S3Properties.getRegionOfEndpoint(pingEndpoint);
        properties.putIfAbsent(S3Properties.REGION, region);

        if (needCheck) {
            String bucketName = properties.get(S3Properties.BUCKET);
            String rootPath = properties.get(S3Properties.ROOT_PATH);
            pingS3(bucketName, rootPath, properties);
        }
        // optional
        S3Properties.optionalS3Property(properties);
    }

    protected static void pingS3(String bucketName, String rootPath, Map<String, String> newProperties)
            throws DdlException {

        Long timestamp = System.currentTimeMillis();
        String prefix = "s3://" + bucketName + "/" + rootPath;
        String testObj = prefix + "/doris-test-object-valid-" + timestamp.toString() + ".txt";

        byte[] contentData = new byte[2 * ObjStorage.CHUNK_SIZE];
        Arrays.fill(contentData, (byte) 'A');
        S3ObjStorage s3ObjStorage = new S3ObjStorage((AbstractS3CompatibleProperties) StorageProperties
                .createPrimary(newProperties));

        Status status = s3ObjStorage.putObject(testObj, new ByteArrayInputStream(contentData), contentData.length);
        if (!Status.OK.equals(status)) {
            String errMsg = "pingS3 failed(put),"
                    + " please check your endpoint, ak/sk or permissions(put/head/delete/list/multipartUpload),"
                    + " status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false);
            throw new DdlException(errMsg);
        }

        status = s3ObjStorage.headObject(testObj);
        if (!Status.OK.equals(status)) {
            String errMsg = "pingS3 failed(head),"
                    + " please check your endpoint, ak/sk or permissions(put/head/delete/list/multipartUpload),"
                    + " status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false);
            throw new DdlException(errMsg);
        }

        RemoteObjects remoteObjects = s3ObjStorage.listObjects(testObj, null);
        LOG.info("remoteObjects: {}", remoteObjects);

        status = s3ObjStorage.multipartUpload(testObj, new ByteArrayInputStream(contentData), contentData.length);
        if (!Status.OK.equals(status)) {
            String errMsg = "pingS3 failed(multipartUpload),"
                    + " please check your endpoint, ak/sk or permissions(put/head/delete/list/multipartUpload),"
                    + " status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false);
            throw new DdlException(errMsg);
        }

        status = s3ObjStorage.deleteObject(testObj);
        if (!Status.OK.equals(status)) {
            String errMsg = "pingS3 failed(delete),"
                    + " please check your endpoint, ak/sk or permissions(put/head/delete/list/multipartUpload),"
                    + " status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false);
            throw new DdlException(errMsg);
        }

        LOG.info("success to ping s3");
    }

    private static void normalizeProperties(Map<String, String> properties, String provider) {
        // Validate the raw aliases before a preferred value can hide conflicting credentials.
        GCSProperties.validateNativeCredentialCompatibility(properties);
        if (provider == null) {
            return;
        }
        switch (provider.toUpperCase(Locale.ROOT)) {
            case "GCP":
                // Normalize before validation, policy checks and persistence, so FE connector
                // binding and Resource/Vault protocol builders consume the same values.
                GCS_PROPERTY_ALIASES.forEach((alias, key) -> {
                    String value = properties.remove(alias);
                    // Match connector binding: nonblank gs.* values take precedence.
                    if (StringUtils.isNotBlank(value)) {
                        properties.put(key, value);
                    }
                });
                break;
            default:
                break;
        }
    }

    @Override
    public synchronized void modifyProperties(Map<String, String> newProperties) throws DdlException {
        // Serialize the snapshot, validation and publication. A lock only around publication
        // would allow a concurrent ALTER to replace a successful update with an older snapshot.
        Map<String, String> properties = new HashMap<>(newProperties);
        String provider = StringUtils.defaultIfEmpty(properties.get(StorageProperties.FS_PROVIDER_KEY),
                this.properties.get(StorageProperties.FS_PROVIDER_KEY));
        // Preserve AWS_* ALTER compatibility before merging with stored canonical properties.
        S3Properties.convertToStdProperties(properties);
        Map<String, String> effectiveProperties = new HashMap<>(this.properties);
        for (Map.Entry<String, String> update : properties.entrySet()) {
            // Match persistence: empty updates are ignored, except when clearing a session token.
            replaceIfEffectiveValue(effectiveProperties, update.getKey(), update.getValue());
            if (S3Properties.SESSION_TOKEN.equals(update.getKey()) || S3Properties.Env.TOKEN.equals(update.getKey())) {
                effectiveProperties.put(update.getKey(), update.getValue());
            }
        }
        // Resolve aliases only after merging: nonblank gs.* values, including stored aliases,
        // take precedence over s3.* values in the final configuration.
        normalizeProperties(effectiveProperties, provider);
        S3Properties.convertToStdProperties(effectiveProperties);

        // Normalize the update keys for modification restrictions and credential-mode changes.
        // Their values must not overwrite the configuration resolved above.
        normalizeProperties(properties, provider);
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3Properties.ENDPOINT, S3Properties.REGION,
                    S3Properties.ROOT_PATH, S3Properties.BUCKET, S3Properties.Env.ENDPOINT, S3Properties.Env.REGION,
                    S3Properties.Env.ROOT_PATH, S3Properties.Env.BUCKET);
            Optional<String> any = cantChangeProperties.stream()
                    .filter(key -> properties.containsKey(key)
                            || !Objects.equals(this.properties.get(key), effectiveProperties.get(key)))
                    .findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        if (!Strings.isNullOrEmpty(effectiveProperties.get(S3Properties.ENDPOINT))) {
            effectiveProperties.put(S3Properties.Env.ENDPOINT, effectiveProperties.get(S3Properties.ENDPOINT));
        }
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            if (kv.getKey().equalsIgnoreCase(S3Properties.ROLE_ARN)
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                effectiveProperties.remove(S3Properties.ACCESS_KEY);
                effectiveProperties.remove(S3Properties.Env.ACCESS_KEY);
                effectiveProperties.remove(S3Properties.SECRET_KEY);
                effectiveProperties.remove(S3Properties.Env.SECRET_KEY);
            }
            if (kv.getKey().equalsIgnoreCase(S3Properties.ACCESS_KEY)
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                effectiveProperties.remove(S3Properties.ROLE_ARN);
                effectiveProperties.remove(S3Properties.Env.ROLE_ARN);
                effectiveProperties.remove(S3Properties.EXTERNAL_ID);
                effectiveProperties.remove(S3Properties.Env.EXTERNAL_ID);
            }
        }
        GCSProperties.validateNativeCredentialCompatibility(effectiveProperties);
        boolean needCheck = isNeedCheck(effectiveProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3Properties.requiredS3PingProperties(effectiveProperties);
            Map<String, String> changedProperties = new HashMap<>(effectiveProperties);
            pingS3(effectiveProperties.get(S3Properties.BUCKET), effectiveProperties.get(S3Properties.ROOT_PATH),
                    changedProperties);
        }

        writeLock();
        try {
            this.properties = effectiveProperties;
            ++version;
        } finally {
            writeUnlock();
        }
        super.modifyProperties(effectiveProperties);
    }

    private CloudCredentialWithEndpoint getS3PingCredentials(Map<String, String> properties) {
        String ak = properties.getOrDefault(S3Properties.ACCESS_KEY, this.properties.get(S3Properties.ACCESS_KEY));
        String sk = properties.getOrDefault(S3Properties.SECRET_KEY, this.properties.get(S3Properties.SECRET_KEY));
        String token = properties.getOrDefault(S3Properties.SESSION_TOKEN,
                this.properties.get(S3Properties.SESSION_TOKEN));
        String endpoint = properties.getOrDefault(S3Properties.ENDPOINT, this.properties.get(S3Properties.ENDPOINT));
        String pingEndpoint = "http://" + endpoint;
        String region = S3Properties.getRegionOfEndpoint(pingEndpoint);
        properties.putIfAbsent(S3Properties.REGION, region);
        return new CloudCredentialWithEndpoint(pingEndpoint, region, ak, sk, token);
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
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "id", String.valueOf(id)));
        readLock();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "version", String.valueOf(version)));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (PrintableMap.HIDDEN_KEY.contains(entry.getKey())) {
                continue;
            }
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3Properties.Env.SECRET_KEY)
                    || entry.getKey().equals(S3Properties.SECRET_KEY)
                    || entry.getKey().equals(S3Properties.Env.TOKEN)
                    || entry.getKey().equals(S3Properties.SESSION_TOKEN)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }
}


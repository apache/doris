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
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.UploadPartResult;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.ObjStorage;
import org.apache.doris.filesystem.spi.RequestBody;
import org.apache.doris.fs.FileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
        this.properties = StorageAdapter.normalizeProperties(newProperties, newProperties);

        // check properties
        S3ResourceCompat.requiredS3PingProperties(properties);
        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }

        String endpoint = properties.get(S3ResourceCompat.ENDPOINT);
        properties.put(S3ResourceCompat.Env.ENDPOINT, endpoint);
        String region = S3ResourceCompat.getRegionOfEndpoint(endpoint);
        properties.putIfAbsent(S3ResourceCompat.REGION, region);

        if (needCheck) {
            Map<String, String> pingProperties = new HashMap<>(properties);
            String pingEndpoint = S3Util.buildEndpointUrl(endpoint);
            pingProperties.put(S3ResourceCompat.ENDPOINT, pingEndpoint);
            pingProperties.put(S3ResourceCompat.Env.ENDPOINT, pingEndpoint);
            String bucketName = properties.get(S3ResourceCompat.BUCKET);
            String rootPath = properties.get(S3ResourceCompat.ROOT_PATH);
            pingS3(bucketName, rootPath, pingProperties);
        }
        // optional
        S3ResourceCompat.optionalS3Property(properties);
    }

    protected static void pingS3(String bucketName, String rootPath, Map<String, String> newProperties)
            throws DdlException {
        // Normalize rootPath: strip leading slashes to avoid "s3://bucket//path" double-slash
        if (rootPath != null) {
            rootPath = rootPath.replaceAll("^/+", "");
        }

        Long timestamp = System.currentTimeMillis();
        String prefix = "s3://" + bucketName + "/" + rootPath;
        String testObj = prefix + "/doris-test-object-valid-" + timestamp.toString() + ".txt";

        // 5MB per chunk — same as the multipart upload minimum
        final int chunkSize = 5 * 1024 * 1024;
        byte[] contentData = new byte[2 * chunkSize];
        Arrays.fill(contentData, (byte) 'A');

        try {
            org.apache.doris.filesystem.FileSystem fileSystem =
                    FileSystemFactory.getFileSystem(newProperties);
            Preconditions.checkState(fileSystem instanceof ObjFileSystem,
                    "Expected object-storage filesystem for S3 resource");
            ObjStorage<?> objStorage = ((ObjFileSystem) fileSystem).getObjStorage();

            try {
                objStorage.putObject(testObj,
                        RequestBody.of(new ByteArrayInputStream(contentData), contentData.length));
            } catch (IOException e) {
                throw new DdlException("pingS3 failed(put),"
                        + " please check your endpoint, ak/sk or permissions"
                        + "(put/head/delete/list/multipartUpload),"
                        + " err: " + e.getMessage() + ", properties: "
                        + new DatasourcePrintableMap<>(newProperties, "=", true, false, true, false));
            }

            try {
                objStorage.headObject(testObj);
            } catch (IOException e) {
                throw new DdlException("pingS3 failed(head),"
                        + " please check your endpoint, ak/sk or permissions"
                        + "(put/head/delete/list/multipartUpload),"
                        + " err: " + e.getMessage() + ", properties: "
                        + new DatasourcePrintableMap<>(newProperties, "=", true, false, true, false));
            }

            try {
                org.apache.doris.filesystem.spi.RemoteObjects remoteObjects =
                        objStorage.listObjects(testObj, null);
                LOG.info("remoteObjects: {}", remoteObjects);
            } catch (IOException e) {
                throw new DdlException("pingS3 failed(list),"
                        + " please check your endpoint, ak/sk or permissions"
                        + "(put/head/delete/list/multipartUpload),"
                        + " err: " + e.getMessage() + ", properties: "
                        + new DatasourcePrintableMap<>(newProperties, "=", true, false, true, false));
            }

            // multipart upload: initiate → upload one part → complete
            try {
                String uploadId = objStorage.initiateMultipartUpload(testObj);
                try {
                    UploadPartResult partResult = objStorage.uploadPart(testObj, uploadId, 1,
                            RequestBody.of(new ByteArrayInputStream(contentData), contentData.length));
                    objStorage.completeMultipartUpload(testObj, uploadId, Collections.singletonList(partResult));
                } catch (IOException e) {
                    try {
                        objStorage.abortMultipartUpload(testObj, uploadId);
                    } catch (Exception ignored) {
                        // best-effort cleanup
                    }
                    throw e;
                }
            } catch (IOException e) {
                throw new DdlException("pingS3 failed(multipartUpload),"
                        + " please check your endpoint, ak/sk or permissions"
                        + "(put/head/delete/list/multipartUpload),"
                        + " err: " + e.getMessage() + ", properties: "
                        + new DatasourcePrintableMap<>(newProperties, "=", true, false, true, false));
            }

            try {
                objStorage.deleteObject(testObj);
            } catch (IOException e) {
                throw new DdlException("pingS3 failed(delete),"
                        + " please check your endpoint, ak/sk or permissions"
                        + "(put/head/delete/list/multipartUpload),"
                        + " err: " + e.getMessage() + ", properties: "
                        + new DatasourcePrintableMap<>(newProperties, "=", true, false, true, false));
            }
        } catch (DdlException e) {
            throw e;
        } catch (IOException e) {
            throw new DdlException("pingS3 failed: " + e.getMessage());
        }

        LOG.info("success to ping s3");
    }

    @Override
    public synchronized void modifyProperties(Map<String, String> newProperties) throws DdlException {
        // Serialize the snapshot, validation and publication. A lock only around publication
        // would allow a concurrent ALTER to replace a successful update with an older snapshot.
        Map<String, String> properties = new HashMap<>(newProperties);
        Map<String, String> selectionProperties = new HashMap<>(this.properties);
        selectionProperties.putAll(properties);
        if (Strings.isNullOrEmpty(properties.get("provider"))) {
            // Empty ALTER values retain the persisted provider, like other resource properties.
            selectionProperties.put("provider", this.properties.get("provider"));
        }
        // Normalize the patch independently so its aliases win over persisted canonical values.
        S3ResourceCompat.convertToStdProperties(properties);
        Map<String, String> normalizedUpdates = StorageAdapter.normalizeProperties(properties, selectionProperties);
        Map<String, String> effectiveProperties =
                StorageAdapter.normalizeProperties(this.properties, selectionProperties);
        S3ResourceCompat.convertToStdProperties(effectiveProperties);
        for (Map.Entry<String, String> update : normalizedUpdates.entrySet()) {
            // Empty updates are ignored, except when clearing a session token or impersonation account.
            replaceIfEffectiveValue(effectiveProperties, update.getKey(), update.getValue());
            if (S3ResourceCompat.SESSION_TOKEN.equals(update.getKey())
                    || S3ResourceCompat.Env.TOKEN.equals(update.getKey())
                    || StorageAdapter.isClearableProperty(update.getKey())) {
                effectiveProperties.put(update.getKey(), update.getValue());
            }
        }
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3ResourceCompat.ENDPOINT, S3ResourceCompat.REGION,
                    S3ResourceCompat.ROOT_PATH, S3ResourceCompat.BUCKET, S3ResourceCompat.Env.ENDPOINT,
                    S3ResourceCompat.Env.REGION,
                    S3ResourceCompat.Env.ROOT_PATH, S3ResourceCompat.Env.BUCKET);
            Optional<String> any = cantChangeProperties.stream()
                    .filter(key -> normalizedUpdates.containsKey(key)
                            || !Objects.equals(this.properties.get(key), effectiveProperties.get(key)))
                    .findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        if (!Strings.isNullOrEmpty(effectiveProperties.get(S3ResourceCompat.ENDPOINT))) {
            effectiveProperties.put(S3ResourceCompat.Env.ENDPOINT, effectiveProperties.get(S3ResourceCompat.ENDPOINT));
        }
        for (Map.Entry<String, String> kv : normalizedUpdates.entrySet()) {
            if (kv.getKey().equalsIgnoreCase(S3ResourceCompat.ROLE_ARN)
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                effectiveProperties.remove(S3ResourceCompat.ACCESS_KEY);
                effectiveProperties.remove(S3ResourceCompat.Env.ACCESS_KEY);
                effectiveProperties.remove(S3ResourceCompat.SECRET_KEY);
                effectiveProperties.remove(S3ResourceCompat.Env.SECRET_KEY);
            }
            if (kv.getKey().equalsIgnoreCase(S3ResourceCompat.ACCESS_KEY)
                    && !Strings.isNullOrEmpty(kv.getValue())) {
                effectiveProperties.remove(S3ResourceCompat.ROLE_ARN);
                effectiveProperties.remove(S3ResourceCompat.Env.ROLE_ARN);
                effectiveProperties.remove(S3ResourceCompat.EXTERNAL_ID);
                effectiveProperties.remove(S3ResourceCompat.Env.EXTERNAL_ID);
            }
        }
        StorageAdapter.resolveAuthentication(effectiveProperties);
        boolean needCheck = isNeedCheck(effectiveProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3ResourceCompat.requiredS3PingProperties(effectiveProperties);
            Map<String, String> changedProperties = new HashMap<>(effectiveProperties);
            String endpoint = S3Util.buildEndpointUrl(changedProperties.get(S3ResourceCompat.ENDPOINT));
            changedProperties.put(S3ResourceCompat.ENDPOINT, endpoint);
            changedProperties.put(S3ResourceCompat.Env.ENDPOINT, endpoint);
            pingS3(effectiveProperties.get(S3ResourceCompat.BUCKET),
                    effectiveProperties.get(S3ResourceCompat.ROOT_PATH),
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
        String ak = properties.getOrDefault(S3ResourceCompat.ACCESS_KEY,
                this.properties.get(S3ResourceCompat.ACCESS_KEY));
        String sk = properties.getOrDefault(S3ResourceCompat.SECRET_KEY,
                this.properties.get(S3ResourceCompat.SECRET_KEY));
        String token = properties.getOrDefault(S3ResourceCompat.SESSION_TOKEN,
                this.properties.get(S3ResourceCompat.SESSION_TOKEN));
        String endpoint = properties.getOrDefault(S3ResourceCompat.ENDPOINT,
                this.properties.get(S3ResourceCompat.ENDPOINT));
        String region = S3ResourceCompat.getRegionOfEndpoint(endpoint);
        properties.putIfAbsent(S3ResourceCompat.REGION, region);
        return new CloudCredentialWithEndpoint(endpoint, region, ak, sk, token);
    }

    private boolean isNeedCheck(Map<String, String> newProperties) {
        boolean needCheck = !this.properties.containsKey(S3ResourceCompat.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(S3ResourceCompat.VALIDITY_CHECK));
        if (newProperties != null && newProperties.containsKey(S3ResourceCompat.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(S3ResourceCompat.VALIDITY_CHECK));
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
            if (DatasourcePrintableMap.HIDDEN_KEY.contains(entry.getKey())) {
                continue;
            }
            // it's dangerous to show password in show odbc resource,
            // so we use empty string to replace the real password
            if (entry.getKey().equals(S3ResourceCompat.Env.SECRET_KEY)
                    || entry.getKey().equals(S3ResourceCompat.SECRET_KEY)
                    || entry.getKey().equals(S3ResourceCompat.Env.TOKEN)
                    || entry.getKey().equals(S3ResourceCompat.SESSION_TOKEN)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }
}


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
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.ObjStorage;
import org.apache.doris.fs.obj.RemoteObjects;
import org.apache.doris.fs.obj.S3ObjStorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        this.properties = Maps.newHashMap(newProperties);

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

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3Properties.ENDPOINT, S3Properties.REGION,
                    S3Properties.ROOT_PATH, S3Properties.BUCKET, S3Properties.Env.ENDPOINT, S3Properties.Env.REGION,
                    S3Properties.Env.ROOT_PATH, S3Properties.Env.BUCKET);
            Optional<String> any = cantChangeProperties.stream().filter(properties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        // compatible with old version, Need convert if modified properties map uses old properties.
        S3Properties.convertToStdProperties(properties);
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3Properties.requiredS3PingProperties(this.properties);
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(properties);
            String bucketName = properties.getOrDefault(S3Properties.BUCKET, this.properties.get(S3Properties.BUCKET));
            String rootPath = properties.getOrDefault(S3Properties.ROOT_PATH,
                    this.properties.get(S3Properties.ROOT_PATH));

            pingS3(bucketName, rootPath, changedProperties);
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(S3Properties.Env.TOKEN)
                    || kv.getKey().equals(S3Properties.SESSION_TOKEN)) {
                this.properties.put(kv.getKey(), kv.getValue());
            }
        }
        ++version;
        writeUnlock();
        super.modifyProperties(properties);
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


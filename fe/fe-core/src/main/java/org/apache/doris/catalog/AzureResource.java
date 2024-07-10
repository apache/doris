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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.fs.remote.AzureFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AzureResource extends Resource {

    private static final Logger LOG = LogManager.getLogger(AzureResource.class);
    private Map<String, String> properties;

    public AzureResource() {
        super();
    }

    public AzureResource(String name) {
        super(name, ResourceType.AZURE);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        // check properties
        S3Properties.requiredS3PingProperties(properties);
        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("azure info need check validity : {}", needCheck);
        }

        // the endpoint for ping need add uri scheme.
        String pingEndpoint = properties.get(S3Properties.ENDPOINT);
        if (!pingEndpoint.startsWith("http://")) {
            pingEndpoint = "http://" + properties.get(S3Properties.ENDPOINT);
            properties.put(S3Properties.ENDPOINT, pingEndpoint);
            properties.put(S3Properties.Env.ENDPOINT, pingEndpoint);
        }
        String region = S3Properties.getRegionOfEndpoint(pingEndpoint);
        properties.putIfAbsent(S3Properties.REGION, region);
        String ak = properties.get(S3Properties.ACCESS_KEY);
        String sk = properties.get(S3Properties.SECRET_KEY);
        String token = properties.get(S3Properties.SESSION_TOKEN);
        CloudCredentialWithEndpoint credential = new CloudCredentialWithEndpoint(pingEndpoint, region, ak, sk, token);

        if (needCheck) {
            String bucketName = properties.get(S3Properties.BUCKET);
            String rootPath = properties.get(S3Properties.ROOT_PATH);
            pingAzure(credential, bucketName, rootPath, properties);
        }
        // optional
        S3Properties.optionalS3Property(properties);
        this.properties = properties;
    }

    private static void pingAzure(CloudCredentialWithEndpoint credential, String bucketName, String rootPath,
            Map<String, String> properties) throws DdlException {
        AzureFileSystem fileSystem = new AzureFileSystem(properties);
        String testFile = rootPath + "/test-object-valid.txt";
        if (FeConstants.runningUnitTest) {
            return;
        }
        Status status = fileSystem.exists(testFile);
        if (status != Status.OK || status.getErrCode() != Status.ErrCode.NOT_FOUND) {
            throw new DdlException(
                    "ping azure failed(head), status: " + status + ", properties: " + new PrintableMap<>(
                            properties, "=", true, false, true, false));
        }

        LOG.info("success to ping azure");
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

            pingAzure(getS3PingCredentials(changedProperties), bucketName, rootPath, changedProperties);
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

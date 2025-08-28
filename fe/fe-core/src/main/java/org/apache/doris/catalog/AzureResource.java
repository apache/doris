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
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.obj.AzureObjStorage;
import org.apache.doris.fs.obj.ObjStorage;
import org.apache.doris.fs.obj.RemoteObjects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AzureResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(AzureResource.class);
    private Map<String, String> properties = Maps.newHashMap();

    public AzureResource() {
        super();
    }

    public AzureResource(String name) {
        super(name, ResourceType.AZURE);
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);
        // check properties
        S3Properties.requiredS3PingProperties(this.properties);
        // default need check resource conf valid, so need fix ut and regression case
        boolean needCheck = isNeedCheck(this.properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("azure info need check validity : {}", needCheck);
        }

        // the endpoint for ping need add uri scheme.
        String pingEndpoint = this.properties.get(S3Properties.ENDPOINT);
        if (!pingEndpoint.startsWith("http://")) {
            pingEndpoint = "http://" + this.properties.get(S3Properties.ENDPOINT);
            this.properties.put(S3Properties.ENDPOINT, pingEndpoint);
            this.properties.put(S3Properties.Env.ENDPOINT, pingEndpoint);
        }

        if (needCheck) {
            String bucketName = this.properties.get(S3Properties.BUCKET);
            String rootPath = this.properties.get(S3Properties.ROOT_PATH);
            pingAzure(bucketName, rootPath, this.properties);
        }
        // optional
        S3Properties.optionalS3Property(this.properties);
    }

    protected static void pingAzure(String bucketName, String rootPath,
            Map<String, String> newProperties) throws DdlException {

        Long timestamp = System.currentTimeMillis();
        String testObj = "azure://" + bucketName + "/" + rootPath
                + "/doris-test-object-valid-" + timestamp.toString() + ".txt";

        byte[] contentData = new byte[2 * ObjStorage.CHUNK_SIZE];
        Arrays.fill(contentData, (byte) 'A');
        AzureProperties azureProperties = (AzureProperties) StorageProperties.createPrimary(newProperties);
        AzureObjStorage azureObjStorage = new AzureObjStorage(azureProperties);

        Status status = azureObjStorage.putObject(testObj, new ByteArrayInputStream(contentData), contentData.length);
        if (!Status.OK.equals(status)) {
            throw new DdlException(
                    "ping azure failed(put), status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false));
        }

        status = azureObjStorage.headObject(testObj);
        if (!Status.OK.equals(status)) {
            throw new DdlException(
                    "ping azure failed(head), status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false));
        }

        RemoteObjects remoteObjects = azureObjStorage.listObjects(testObj, null);
        LOG.info("remoteObjects: {}", remoteObjects);
        Preconditions.checkArgument(remoteObjects.getObjectList().size() == 1, "remoteObjects.size() must equal 1");

        status = azureObjStorage.deleteObject(testObj);
        if (!Status.OK.equals(status)) {
            throw new DdlException(
                    "ping azure failed(delete), status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false));
        }

        status = azureObjStorage.multipartUpload(testObj,
                new ByteArrayInputStream(contentData), contentData.length);
        if (!Status.OK.equals(status)) {
            throw new DdlException(
                    "ping azure failed(multiPartPut), status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false));
        }

        status = azureObjStorage.deleteObject(testObj);
        if (!Status.OK.equals(status)) {
            throw new DdlException(
                    "ping azure failed(delete), status: " + status + ", properties: " + new PrintableMap<>(
                            newProperties, "=", true, false, true, false));
        }
        LOG.info("Success to ping azure blob storage.");
    }

    @Override
    public void modifyProperties(Map<String, String> newProperties) throws DdlException {
        if (references.containsValue(ReferenceType.POLICY)) {
            // can't change, because remote fs use it info to find data.
            List<String> cantChangeProperties = Arrays.asList(S3Properties.ENDPOINT, S3Properties.REGION,
                    S3Properties.ROOT_PATH, S3Properties.BUCKET, S3Properties.Env.ENDPOINT, S3Properties.Env.REGION,
                    S3Properties.Env.ROOT_PATH, S3Properties.Env.BUCKET);
            Optional<String> any = cantChangeProperties.stream().filter(newProperties::containsKey).findAny();
            if (any.isPresent()) {
                throw new DdlException("current not support modify property : " + any.get());
            }
        }
        // compatible with old version, Need convert if modified properties map uses old properties.
        S3Properties.convertToStdProperties(newProperties);
        boolean needCheck = isNeedCheck(newProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("s3 info need check validity : {}", needCheck);
        }
        if (needCheck) {
            S3Properties.requiredS3PingProperties(this.properties);
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(newProperties);
            String bucketName = newProperties.getOrDefault(S3Properties.BUCKET,
                    this.properties.get(S3Properties.BUCKET));
            String rootPath = newProperties.getOrDefault(S3Properties.ROOT_PATH,
                    this.properties.get(S3Properties.ROOT_PATH));

            pingAzure(bucketName, rootPath, changedProperties);
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : newProperties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(S3Properties.Env.TOKEN)
                    || kv.getKey().equals(S3Properties.SESSION_TOKEN)) {
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

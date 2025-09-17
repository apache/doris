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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TPushStoragePolicyReq;
import org.apache.doris.thrift.TS3StorageParam;
import org.apache.doris.thrift.TStoragePolicy;
import org.apache.doris.thrift.TStorageResource;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PushStoragePolicyTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushStoragePolicyTask.class);

    private List<Policy> storagePolicy;
    private List<Resource> resource;
    private List<Long> droppedStoragePolicy;

    public PushStoragePolicyTask(long backendId, List<Policy> storagePolicy,
                                 List<Resource> resource, List<Long> droppedStoragePolicy) {
        super(null, backendId, TTaskType.PUSH_STORAGE_POLICY, -1, -1, -1, -1, -1, -1, -1);
        this.storagePolicy = storagePolicy;
        this.resource = resource;
        this.droppedStoragePolicy = droppedStoragePolicy;
    }

    public TPushStoragePolicyReq toThrift() {
        TPushStoragePolicyReq ret = new TPushStoragePolicyReq();
        List<TStoragePolicy> tStoragePolicies = new ArrayList<>();
        storagePolicy.forEach(p -> {
            TStoragePolicy item = new TStoragePolicy();
            p.readLock();
            try {
                item.setId(p.getId());
                item.setName(p.getPolicyName());
                item.setVersion(p.getVersion());
                StoragePolicy storagePolicy = (StoragePolicy) p;
                String resourceName = storagePolicy.getStorageResource();
                Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
                if (resource == null || (resource.getType() != ResourceType.S3
                        && resource.getType() != ResourceType.HDFS)) {
                    LOG.warn("can't find s3 resource or hdfs resource by name {}", resourceName);
                    return;
                }
                item.setResourceId(resource.getId());
                long coolDownDatetime = storagePolicy.getCooldownTimestampMs() / 1000;
                item.setCooldownDatetime(coolDownDatetime);
                long coolDownTtl = storagePolicy.getCooldownTtl();
                item.setCooldownTtl(coolDownTtl);
            } finally {
                p.readUnlock();
            }
            tStoragePolicies.add(item);
        });
        ret.setStoragePolicy(tStoragePolicies);

        List<TStorageResource> tStorageResources = new ArrayList<>();
        resource.forEach(r -> {
            TStorageResource item = new TStorageResource();
            r.readLock();
            item.setId(r.getId());
            item.setName(r.getName());
            item.setVersion(r.getVersion());
            if (r.getType() == ResourceType.S3) {
                item.setS3StorageParam(getS3TStorageParam(r.getCopiedProperties()));
            } else if (r.getType() == ResourceType.HDFS) {
                item.setHdfsStorageParam(HdfsResource.generateHdfsParam(r.getCopiedProperties()));
            }
            r.readUnlock();
            tStorageResources.add(item);
        });
        ret.setResource(tStorageResources);

        ret.setDroppedStoragePolicy(droppedStoragePolicy);
        return ret;
    }

    private static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam s3Info = new TS3StorageParam();

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            s3Info.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                s3Info.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            s3Info.setCredProviderType(TCredProviderType.INSTANCE_PROFILE);
        }

        s3Info.setEndpoint(properties.get(S3Properties.ENDPOINT));
        s3Info.setRegion(properties.get(S3Properties.REGION));
        s3Info.setAk(properties.get(S3Properties.ACCESS_KEY));
        s3Info.setSk(properties.get(S3Properties.SECRET_KEY));
        s3Info.setToken(properties.get(S3Properties.SESSION_TOKEN));

        s3Info.setRootPath(properties.get(S3Properties.ROOT_PATH));
        s3Info.setBucket(properties.get(S3Properties.BUCKET));
        String maxConnections = properties.get(S3Properties.MAX_CONNECTIONS);
        s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                ? S3Properties.Env.DEFAULT_MAX_CONNECTIONS : maxConnections));
        String requestTimeoutMs = properties.get(S3Properties.REQUEST_TIMEOUT_MS);
        s3Info.setRequestTimeoutMs(Integer.parseInt(requestTimeoutMs == null
                ? S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS : requestTimeoutMs));
        String connTimeoutMs = properties.get(S3Properties.CONNECTION_TIMEOUT_MS);
        s3Info.setConnTimeoutMs(Integer.parseInt(connTimeoutMs == null
                ? S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS : connTimeoutMs));
        String usePathStyle = properties.getOrDefault(S3Properties.USE_PATH_STYLE, "false");
        s3Info.setUsePathStyle(Boolean.parseBoolean(usePathStyle));
        return s3Info;
    }
}

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
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.thrift.TPushStoragePolicyReq;
import org.apache.doris.thrift.TS3StorageParam;
import org.apache.doris.thrift.TStoragePolicy;
import org.apache.doris.thrift.TStorageResource;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PushStoragePolicyTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushStoragePolicyTask.class);

    private List<Policy> storagePolicy = new ArrayList<>();
    private List<Resource> resource = new ArrayList<>();
    private List<Long> droppedStoragePolicy = new ArrayList<>();

    public PushStoragePolicyTask(long backendId, List<Policy> storagePolicy,
                                 List<Resource> resource, List<Long> droppedStoragePolicy) {
        super(null, backendId, TTaskType.PUSH_STORAGE_POLICY, -1, -1, -1, -1, -1, -1, -1);
        this.storagePolicy.addAll(storagePolicy);
        this.resource.addAll(resource);
        this.droppedStoragePolicy.addAll(droppedStoragePolicy);
    }

    public TPushStoragePolicyReq toThrift() {
        TPushStoragePolicyReq ret = new TPushStoragePolicyReq();
        List<TStoragePolicy> tsp = new ArrayList<>();
        storagePolicy.forEach(p -> {
            TStoragePolicy item = new TStoragePolicy();
            item.setId(p.getPolicyId());
            item.setName(p.getPolicyName());
            item.setVersion(p.getVersion());
            String resourceName = ((StoragePolicy) p).getStorageResource();
            List<Resource> allS3Resource = Env.getCurrentEnv().getResourceMgr().getAllS3Resource();
            Optional<Resource> s3 = allS3Resource.stream().filter(r -> r.getName().equals(resourceName)).findAny();
            if (!s3.isPresent()) {
                LOG.warn("can't find s3 resource by name {}", resourceName);
                return;
            }
            item.setResourceId(s3.get().getResourceId());
            long coolDownDatetime = ((StoragePolicy) p).getCooldownTimestampMs() / 1000;
            item.setCooldownDatetime(coolDownDatetime);
            long coolDownTtl = ((StoragePolicy) p).getCooldownTtlMs() / 1000;
            item.setCooldownTtl(coolDownTtl);
        });
        ret.setStoragePolicy(tsp);

        List<TStorageResource> tsr = new ArrayList<>();
        resource.forEach(r -> {
            TStorageResource item = new TStorageResource();
            item.setId(r.getResourceId());
            item.setName(r.getName());
            item.setVersion(r.getVersion());
            TS3StorageParam s3Info = new TS3StorageParam();
            s3Info.setEndpoint(((S3Resource) r).getProperty(S3Resource.S3_ENDPOINT));
            s3Info.setRegion(((S3Resource) r).getProperty(S3Resource.S3_REGION));
            s3Info.setAk(((S3Resource) r).getProperty(S3Resource.S3_ACCESS_KEY));
            s3Info.setSk(((S3Resource) r).getProperty(S3Resource.S3_SECRET_KEY));
            s3Info.setRootPath(((S3Resource) r).getProperty(S3Resource.S3_ROOT_PATH));
            s3Info.setBucket(((S3Resource) r).getProperty(S3Resource.S3_BUCKET));
            String maxConnections = ((S3Resource) r).getProperty(S3Resource.S3_MAX_CONNECTIONS);
            s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                    ? S3Resource.DEFAULT_S3_MAX_CONNECTIONS : maxConnections));
            String requestTimeoutMs = ((S3Resource) r).getProperty(S3Resource.S3_REQUEST_TIMEOUT_MS);
            s3Info.setMaxConn(Integer.parseInt(requestTimeoutMs == null
                    ? S3Resource.DEFAULT_S3_REQUEST_TIMEOUT_MS : requestTimeoutMs));
            String connTimeoutMs = ((S3Resource) r).getProperty(S3Resource.S3_CONNECTION_TIMEOUT_MS);
            s3Info.setMaxConn(Integer.parseInt(connTimeoutMs == null
                    ? S3Resource.DEFAULT_S3_CONNECTION_TIMEOUT_MS : connTimeoutMs));
            item.setS3StorageParam(s3Info);
        });
        ret.setResource(tsr);

        ret.setDroppedStoragePolicy(droppedStoragePolicy);

        LOG.info("TPushStoragePolicyReq toThrift : {}", ret);
        return ret;
    }
}

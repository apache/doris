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
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.thrift.TPushStoragePolicyReq;
import org.apache.doris.thrift.TStoragePolicy;
import org.apache.doris.thrift.TStorageResource;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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
                item.setS3StorageParam(S3Properties.getS3TStorageParam(r.getCopiedProperties()));
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
}

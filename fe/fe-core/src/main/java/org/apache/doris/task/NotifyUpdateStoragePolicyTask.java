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

import org.apache.doris.catalog.S3Resource;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.thrift.TGetStoragePolicy;
import org.apache.doris.thrift.TS3StorageParam;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class NotifyUpdateStoragePolicyTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(NotifyUpdateStoragePolicyTask.class);
    private String policyName;
    private Map<String, String> properties;

    public NotifyUpdateStoragePolicyTask(long backendId, String name, Map<String, String> properties) {
        super(null, backendId, TTaskType.NOTIFY_UPDATE_STORAGE_POLICY, -1, -1, -1, -1, -1, -1, -1);
        this.policyName = name;
        this.properties = properties;
    }

    public TGetStoragePolicy toThrift() {
        TGetStoragePolicy ret = new TGetStoragePolicy();

        ret.policy_name = policyName;
        // cooldown_datetime in BE is in seconds
        ret.cooldown_datetime = Long.parseLong(properties.get(StoragePolicy.COOLDOWN_DATETIME)) / 1000;
        ret.cooldown_ttl = Long.parseLong(properties.get(StoragePolicy.COOLDOWN_TTL));
        ret.s3_storage_param = new TS3StorageParam();
        ret.s3_storage_param.s3_max_conn = Integer.parseInt(
                properties.getOrDefault(S3Resource.S3_MAX_CONNECTIONS, S3Resource.DEFAULT_S3_MAX_CONNECTIONS));
        ret.s3_storage_param.s3_request_timeout_ms = Integer.parseInt(
                properties.getOrDefault(S3Resource.S3_REQUEST_TIMEOUT_MS, S3Resource.DEFAULT_S3_REQUEST_TIMEOUT_MS));
        ret.s3_storage_param.s3_conn_timeout_ms = Integer.parseInt(
                properties.getOrDefault(S3Resource.S3_CONNECTION_TIMEOUT_MS,
                        S3Resource.DEFAULT_S3_CONNECTION_TIMEOUT_MS));
        ret.s3_storage_param.s3_endpoint = properties.get(S3Resource.S3_ENDPOINT);
        ret.s3_storage_param.s3_region = properties.get(S3Resource.S3_REGION);
        ret.s3_storage_param.root_path = properties.get(S3Resource.S3_ROOT_PATH);
        ret.s3_storage_param.s3_ak = properties.get(S3Resource.S3_ACCESS_KEY);
        ret.s3_storage_param.s3_sk = properties.get(S3Resource.S3_SECRET_KEY);
        ret.s3_storage_param.bucket = properties.get(S3Resource.S3_BUCKET);
        ret.md5_checksum = properties.get(StoragePolicy.MD5_CHECKSUM);

        LOG.info("TGetStoragePolicy toThrift : {}", ret);
        return ret;
    }
}

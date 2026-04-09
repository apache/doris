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

package org.apache.doris.cloud.storage;

import org.apache.doris.cloud.proto.Cloud;

/**
 * Holds object storage credentials and connection info for Cloud stage/copy operations.
 * Extracted from {@code RemoteBase.ObjectInfo}.
 */
public class ObjectInfo {
    private final Cloud.ObjectStoreInfoPB.Provider provider;
    private final String ak;
    private final String sk;
    private final String bucket;
    private final String endpoint;
    private final String region;
    private final String prefix;
    // used when access_type is IAM
    // In OBS, role name is agency name, arn is domain name.
    private final String roleName;
    private final String arn;
    // only used for aws
    private final String externalId;
    private final String token;

    // Used to get sts token
    public ObjectInfo(Cloud.ObjectStoreInfoPB.Provider provider, String ak, String sk,
                      String endpoint, String region, String roleName, String arn, String externalId) {
        this(provider, ak, sk, null, endpoint, region, null, roleName, arn, externalId, null);
    }

    // Used by UT
    public ObjectInfo(Cloud.ObjectStoreInfoPB.Provider provider,
                      String ak, String sk, String bucket, String endpoint, String region, String prefix) {
        this(provider, ak, sk, bucket, endpoint, region, prefix, null, null, null, null);
    }

    // Used by upload for internal stage
    public ObjectInfo(Cloud.ObjectStoreInfoPB objectStoreInfoPB) {
        this(objectStoreInfoPB.getProvider(), objectStoreInfoPB.getAk(), objectStoreInfoPB.getSk(),
                objectStoreInfoPB.getBucket(), objectStoreInfoPB.getEndpoint(), objectStoreInfoPB.getRegion(),
                objectStoreInfoPB.getPrefix());
    }

    public ObjectInfo(Cloud.ObjectStoreInfoPB objectStoreInfoPB, String roleName, String arn,
            String externalId, String token) {
        this(objectStoreInfoPB.getProvider(), objectStoreInfoPB.getAk(), objectStoreInfoPB.getSk(),
                objectStoreInfoPB.getBucket(), objectStoreInfoPB.getEndpoint(), objectStoreInfoPB.getRegion(),
                objectStoreInfoPB.getPrefix(), roleName, arn, externalId, token);
    }

    public ObjectInfo(Cloud.ObjectStoreInfoPB.Provider provider, String ak, String sk, String bucket,
            String endpoint, String region, String prefix, String roleName, String arn, String externalId,
            String token) {
        this.provider = provider;
        this.ak = ak;
        this.sk = sk;
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.region = region;
        this.prefix = prefix;
        this.roleName = roleName;
        this.arn = arn;
        this.externalId = externalId;
        this.token = token;
    }

    public Cloud.ObjectStoreInfoPB.Provider getProvider() {
        return provider;
    }

    public String getAk() {
        return ak;
    }

    public String getSk() {
        return sk;
    }

    public String getBucket() {
        return bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getRoleName() {
        return roleName;
    }

    public String getArn() {
        return arn;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getToken() {
        return token;
    }

    @Override
    public String toString() {
        return "Obj{"
            + "provider=" + provider
            + ", ak='" + ak + '\''
            + ", sk='******" + '\''
            + ", bucket='" + bucket + '\''
            + ", endpoint='" + endpoint + '\''
            + ", region='" + region + '\''
            + ", prefix='" + prefix + '\''
            + ", roleName='" + roleName + '\''
            + ", arn='" + arn + '\''
            + ", externalId='" + externalId + '\''
            + ", token='" + token + '\''
            + '}';
    }
}

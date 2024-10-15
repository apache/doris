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
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageAccessType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;

public abstract class RemoteBase {
    private static final Logger LOG = LogManager.getLogger(RemoteBase.class);

    @Getter
    public static class ObjectInfo {
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

        private ObjectInfo(Cloud.ObjectStoreInfoPB objectStoreInfoPB, String roleName, String arn,
                String externalId, String token) {
            this(objectStoreInfoPB.getProvider(), objectStoreInfoPB.getAk(), objectStoreInfoPB.getSk(),
                    objectStoreInfoPB.getBucket(), objectStoreInfoPB.getEndpoint(), objectStoreInfoPB.getRegion(),
                    objectStoreInfoPB.getPrefix(), roleName, arn, externalId, token);
        }

        private ObjectInfo(Cloud.ObjectStoreInfoPB.Provider provider, String ak, String sk, String bucket,
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

        @Override
        public String toString() {
            return "Obj{"
                + "provider=" + provider
                + ", ak='" + ak + '\''
                + ", sk='" + sk + '\''
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

    public ObjectInfo obj;

    protected static long SESSION_EXPIRE_SECOND = 3600;

    public RemoteBase(ObjectInfo obj) {
        this.obj = obj;
    }

    public String getPresignedUrl(String fileName) {
        return "not impl";
    }

    public abstract ListObjectsResult listObjects(String continuationToken) throws DdlException;

    public abstract ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException;

    public abstract ListObjectsResult headObject(String subKey) throws DdlException;

    public abstract Triple<String, String, String> getStsToken() throws DdlException;

    public abstract void deleteObjects(List<String> keys) throws DdlException;

    public void close() {}

    public static RemoteBase newInstance(ObjectInfo obj) throws Exception {
        switch (obj.provider) {
            case OSS:
                return new OssRemote(obj);
            case GCP:
            case S3:
                return new S3Remote(obj);
            case COS:
                return new CosRemote(obj);
            case OBS:
                return new ObsRemote(obj);
            case BOS:
                return new BosRemote(obj);
            case AZURE:
                return new AzureRemote(obj);
            default:
                throw new Exception("current not support obj : " + obj.toString());
        }
    }

    protected String normalizePrefix() {
        return obj.prefix.isEmpty() ? "" : (obj.prefix.endsWith("/") ? obj.prefix : String.format("%s/", obj.prefix));
    }

    protected String normalizePrefix(String subPrefix) {
        String prefix = normalizePrefix();
        // if prefix is not empty, prefix contains '/' in the end
        return prefix.isEmpty() ? subPrefix : String.format("%s%s", prefix, subPrefix);
    }

    protected String getRelativePath(String key) throws DdlException {
        String expectedPrefix = normalizePrefix();
        if (!key.startsWith(expectedPrefix)) {
            throw new DdlException(
                    "List a object whose key: " + key + " does not start with object prefix: " + expectedPrefix);
        }
        return key.substring(expectedPrefix.length());
    }

    // The etag returned by S3 SDK contains quota in the head and tail, such as "9de7058b7d5816b72d90544810740c1c"
    // The etag returned by OSS SDK does not contain quota, such as 9de7058b7d5816b72d90544810740c1c
    // So add quota for etag returned by OSS SDK.
    protected String formatEtag(String etag) {
        if (!etag.startsWith("\"") && !etag.endsWith("\"")) {
            return String.format("\"%s\"", etag);
        }
        if (etag.startsWith("\"") && etag.endsWith("\"")) {
            return etag;
        }
        throw new IllegalArgumentException("Invalid etag=" + etag);
    }

    public static String getNewRoleSessionName() {
        return "role-" + System.currentTimeMillis();
    }

    public static int getDurationSeconds() {
        return Config.sts_duration;
    }

    public static ObjectInfo analyzeStageObjectStoreInfo(StagePB stagePB) throws AnalysisException {
        if (!stagePB.hasAccessType() || stagePB.getAccessType() == StageAccessType.AKSK
                || stagePB.getAccessType() == StageAccessType.BUCKET_ACL) {
            return new ObjectInfo(stagePB.getObjInfo());
        }
        // accessType == StageAccessType.ARN
        RemoteBase remote = null;
        try {
            ObjectStoreInfoPB infoPB = stagePB.getObjInfo();
            String encodedExternalId = encodeExternalId(stagePB.getExternalId());
            LOG.info("Before parse object storage info={}, encodedExternalId={}", stagePB, encodedExternalId);
            remote = RemoteBase.newInstance(new ObjectInfo(infoPB, stagePB.getRoleName(), stagePB.getArn(),
                    encodedExternalId, null));
            Triple<String, String, String> stsToken = remote.getStsToken();
            ObjectInfo objInfo = new ObjectInfo(infoPB.getProvider(), stsToken.getLeft(), stsToken.getMiddle(),
                    infoPB.getBucket(), infoPB.getEndpoint(), infoPB.getRegion(), infoPB.getPrefix(),
                    stagePB.getRoleName(), stagePB.getArn(), encodedExternalId, stsToken.getRight());
            LOG.info("Parse object storage info, before={}, after={}", new ObjectInfo(infoPB), objInfo);
            return objInfo;
        } catch (Throwable e) {
            LOG.warn("Failed analyze stagePB={}", stagePB, e);
            throw new AnalysisException("Failed analyze object info of stagePB, " + e.getMessage());
        } finally {
            if (remote != null) {
                remote.close();
            }
        }
    }

    private static String encodeExternalId(String externalId) throws UnsupportedEncodingException {
        return Base64.getEncoder().encodeToString(externalId.getBytes("UTF-8"));
    }

    protected void checkDeleteKeys(List<String> keys) throws DdlException {
        if (!checkStagePrefix(obj.getPrefix())) {
            throw new DdlException("Stage prefix: " + obj.getPrefix() + " is invalid");
        }
        for (String key : keys) {
            if (!key.startsWith(obj.getPrefix() + "/")) {
                throw new DdlException(
                        "Delete key:" + key + " is not start with stage prefix: " + obj.getPrefix() + "/");
            }
        }
    }

    public static boolean checkStagePrefix(String stagePrefix) {
        // stage prefix is like: instance_prefix/stage/user_name/user_id
        String[] split = stagePrefix.split("/");
        if (split.length < 3) {
            return false;
        }
        return split[split.length - 3].equals("stage");
    }
}

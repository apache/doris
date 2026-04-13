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

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageAccessType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.COSProperties;
import org.apache.doris.datasource.property.storage.OBSProperties;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.StsCredentials;
import org.apache.doris.fs.FileSystemFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts {@link ObjectInfo} (cloud-specific credential holder) to the
 * corresponding {@link StorageProperties} subclass so that callers can obtain an
 * {@code ObjFileSystem} via {@code FileSystemFactory.get(props)}.
 *
 * <p>STS-related parameters (role ARN, role name, external ID) are carried via the
 * origProps map under the keys defined as public constants below. Each
 * {@code ObjStorage} implementation retrieves them via
 * {@code storageProperties.getOrigProps().get(STS_ROLE_ARN_KEY)}, etc.
 */
public class ObjectInfoAdapter {
    private static final Logger LOG = LogManager.getLogger(ObjectInfoAdapter.class);

    /** Key for the IAM/STS role ARN (AWS/COS) or domain name (OBS). */
    public static final String STS_ROLE_ARN_KEY    = "sts.role_arn";
    /** Key for the IAM/STS role name / agency name (OBS). */
    public static final String STS_ROLE_NAME_KEY   = "sts.role_name";
    /** Key for the AWS STS external ID (Base64-encoded). */
    public static final String STS_EXTERNAL_ID_KEY = "sts.external_id";

    /** Session validity in seconds (mirrors the former {@code RemoteBase.SESSION_EXPIRE_SECOND}). */
    public static long SESSION_EXPIRE_SECOND = 3600;

    private ObjectInfoAdapter() {}

    /**
     * Converts an {@link ObjectInfo} to the matching {@link StorageProperties}
     * subclass. The returned instance can be passed directly to
     * {@code FileSystemFactory.get(props)}.
     *
     * <p>Note: {@code objectInfo.prefix} is <em>not</em> injected into the
     * {@link StorageProperties} — it is a stage-level concept. Callers must pass
     * it separately to {@code listObjectsWithPrefix} / {@code headObjectWithMeta}.
     */
    public static StorageProperties toStorageProperties(ObjectInfo obj) {
        switch (obj.getProvider()) {
            case OSS:
                return OSSProperties.of(buildS3CompatibleProps(obj));
            case S3:
            case GCP:
                return S3Properties.of(buildS3CompatibleProps(obj));
            case COS:
                return COSProperties.of(buildS3CompatibleProps(obj));
            case OBS:
                return OBSProperties.of(buildS3CompatibleProps(obj));
            case BOS:
                // BOS uses S3-compatible endpoints; S3Properties handles it
                return S3Properties.of(buildS3CompatibleProps(obj));
            case TOS:
                // TOS uses S3-compatible endpoints; no STS/Presigned support
                return S3Properties.of(buildS3CompatibleProps(obj));
            case AZURE:
                return AzureProperties.of(buildAzureProps(obj));
            default:
                throw new IllegalArgumentException("Unsupported provider: " + obj.getProvider());
        }
    }

    /**
     * Analyzes stage object storage info, refreshing STS credentials when the stage
     * uses ARN-based access. Replaces {@code RemoteBase.analyzeStageObjectStoreInfo}.
     */
    public static ObjectInfo analyzeStageObjectStoreInfo(StagePB stagePB) throws AnalysisException {
        if (!stagePB.hasAccessType() || stagePB.getAccessType() == StageAccessType.AKSK
                || stagePB.getAccessType() == StageAccessType.BUCKET_ACL) {
            return new ObjectInfo(stagePB.getObjInfo());
        }
        // accessType == StageAccessType.ARN
        try {
            ObjectStoreInfoPB infoPB = stagePB.getObjInfo();
            String encodedExternalId = encodeExternalId(stagePB.getExternalId());
            LOG.info("Before parse object storage info={}, encodedExternalId={}", stagePB, encodedExternalId);
            ObjectInfo arnObj = new ObjectInfo(infoPB, stagePB.getRoleName(), stagePB.getArn(),
                    encodedExternalId, null);
            StorageProperties props = toStorageProperties(arnObj);
            try (ObjFileSystem fs = (ObjFileSystem) FileSystemFactory.getFileSystem(props)) {
                StsCredentials stsToken = fs.getStsToken();
                ObjectInfo objInfo = new ObjectInfo(infoPB.getProvider(), stsToken.getAccessKey(),
                        stsToken.getSecretKey(), infoPB.getBucket(), infoPB.getEndpoint(), infoPB.getRegion(),
                        infoPB.getPrefix(), stagePB.getRoleName(), stagePB.getArn(), encodedExternalId,
                        stsToken.getSecurityToken());
                LOG.info("Parse object storage info, before={}, after={}", new ObjectInfo(infoPB), objInfo);
                return objInfo;
            }
        } catch (Throwable e) {
            LOG.warn("Failed analyze stagePB={}", stagePB, e);
            throw new AnalysisException("Failed analyze object info of stagePB, " + e.getMessage());
        }
    }

    /**
     * Validates that the stage prefix follows the expected layout:
     * {@code <instance_prefix>/stage/<user_name>/<user_id>}.
     * Replaces {@code RemoteBase.checkStagePrefix}.
     */
    public static boolean checkStagePrefix(String stagePrefix) {
        // stage prefix is like: instance_prefix/stage/user_name/user_id
        String[] split = stagePrefix.split("/");
        if (split.length < 3) {
            return false;
        }
        return split[split.length - 3].equals("stage");
    }

    /** Returns the configured STS session duration in seconds. */
    public static int getDurationSeconds() {
        return Config.sts_duration;
    }

    /** Generates a unique STS role session name. */
    public static String getNewRoleSessionName() {
        return "role-" + System.currentTimeMillis();
    }

    // ----------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------

    private static Map<String, String> buildS3CompatibleProps(ObjectInfo obj) {
        Map<String, String> props = new HashMap<>();
        putIfNotBlank(props, "s3.access_key",    obj.getAk());
        putIfNotBlank(props, "s3.secret_key",    obj.getSk());
        putIfNotBlank(props, "s3.endpoint",      obj.getEndpoint());
        putIfNotBlank(props, "s3.region",        obj.getRegion());
        putIfNotBlank(props, "s3.bucket",        obj.getBucket());
        // STS temporary session token (set after a successful getStsToken call)
        putIfNotBlank(props, "s3.session_token", obj.getToken());
        // STS parameters — stored in origProps, read by ObjStorage sub-classes
        putIfNotBlank(props, STS_ROLE_NAME_KEY,    obj.getRoleName());
        putIfNotBlank(props, STS_ROLE_ARN_KEY,     obj.getArn());
        putIfNotBlank(props, STS_EXTERNAL_ID_KEY,  obj.getExternalId());
        return props;
    }

    private static Map<String, String> buildAzureProps(ObjectInfo obj) {
        Map<String, String> props = new HashMap<>();
        // Azure maps ak→accountName, sk→accountKey
        putIfNotBlank(props, "azure.account_name", obj.getAk());
        putIfNotBlank(props, "azure.account_key",  obj.getSk());
        putIfNotBlank(props, "azure.endpoint",     obj.getEndpoint());
        putIfNotBlank(props, "azure.container",    obj.getBucket());
        // SAS token (populated after getStsToken)
        putIfNotBlank(props, "azure.sas_token",    obj.getToken());
        putIfNotBlank(props, STS_ROLE_NAME_KEY,    obj.getRoleName());
        putIfNotBlank(props, STS_ROLE_ARN_KEY,     obj.getArn());
        putIfNotBlank(props, STS_EXTERNAL_ID_KEY,  obj.getExternalId());
        return props;
    }

    private static void putIfNotBlank(Map<String, String> map, String key, String value) {
        if (value != null && !value.isEmpty()) {
            map.put(key, value);
        }
    }

    private static String encodeExternalId(String externalId) throws UnsupportedEncodingException {
        return Base64.getEncoder().encodeToString(externalId.getBytes("UTF-8"));
    }
}

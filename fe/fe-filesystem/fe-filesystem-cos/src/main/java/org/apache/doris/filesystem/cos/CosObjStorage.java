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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.http.HttpMethodName;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.sts.v20180813.StsClient;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleRequest;
import com.tencentcloudapi.sts.v20180813.models.AssumeRoleResponse;
import com.tencentcloudapi.sts.v20180813.models.Credentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Tencent Cloud COS implementation of {@link org.apache.doris.filesystem.spi.ObjStorage}.
 *
 * <p>Extends {@link S3ObjStorage} so all core I/O operations (list, head, put, delete, copy,
 * multipart upload) delegate to the parent using S3-compatible APIs — matching the reference pattern
 * where {@code CosRemote extends DefaultRemote} (the S3-backed base class).
 *
 * <p>The two cloud-specific extension methods that <em>require</em> a native SDK are overridden:
 * <ul>
 *   <li>{@link #getPresignedUrl(String)} — uses Tencent COS SDK
 *       ({@code COSClient.generatePresignedUrl}) to produce the correct COS signature format.</li>
 *   <li>{@link #getStsToken()} — uses Tencent Cloud STS SDK
 *       ({@code StsClient.AssumeRole}) to call {@code sts.tencentcloudapi.com}.</li>
 * </ul>
 *
 * <p>Recognized property keys (COS-specific; AWS_* equivalents accepted as fallback):
 * <ul>
 *   <li>{@code COS_ENDPOINT} / {@code AWS_ENDPOINT} — COS endpoint URL</li>
 *   <li>{@code COS_ACCESS_KEY} / {@code AWS_ACCESS_KEY} — SecretId</li>
 *   <li>{@code COS_SECRET_KEY} / {@code AWS_SECRET_KEY} — SecretKey</li>
 *   <li>{@code COS_BUCKET} / {@code AWS_BUCKET} — bucket name (required for cloud extensions)</li>
 *   <li>{@code COS_REGION} / {@code AWS_REGION} — region (e.g. {@code ap-guangzhou})</li>
 *   <li>{@code COS_ROLE_ARN} / {@code AWS_ROLE_ARN} — role ARN for STS assumption</li>
 * </ul>
 */
public class CosObjStorage extends S3ObjStorage {

    private static final Logger LOG = LogManager.getLogger(CosObjStorage.class);

    /** Validity period for presigned URLs and STS tokens, in seconds. */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> cosProperties;
    private volatile COSClient cosClient;

    public CosObjStorage(Map<String, String> properties) {
        super(toS3Props(properties));
        this.cosProperties = properties;
    }

    /**
     * Translates COS-specific property keys to the AWS keys expected by {@link S3ObjStorage}.
     * If both forms are present, the AWS_* key takes precedence.
     */
    static Map<String, String> toS3Props(Map<String, String> cosProps) {
        Map<String, String> s3Props = new HashMap<>(cosProps);
        if (cosProps.containsKey("COS_ENDPOINT") && !cosProps.containsKey("AWS_ENDPOINT")) {
            s3Props.put("AWS_ENDPOINT", cosProps.get("COS_ENDPOINT"));
        }
        if (cosProps.containsKey("COS_ACCESS_KEY") && !cosProps.containsKey("AWS_ACCESS_KEY")) {
            s3Props.put("AWS_ACCESS_KEY", cosProps.get("COS_ACCESS_KEY"));
        }
        if (cosProps.containsKey("COS_SECRET_KEY") && !cosProps.containsKey("AWS_SECRET_KEY")) {
            s3Props.put("AWS_SECRET_KEY", cosProps.get("COS_SECRET_KEY"));
        }
        if (cosProps.containsKey("COS_BUCKET") && !cosProps.containsKey("AWS_BUCKET")) {
            s3Props.put("AWS_BUCKET", cosProps.get("COS_BUCKET"));
        }
        if (cosProps.containsKey("COS_REGION") && !cosProps.containsKey("AWS_REGION")) {
            s3Props.put("AWS_REGION", cosProps.get("COS_REGION"));
        }
        if (cosProps.containsKey("COS_ROLE_ARN") && !cosProps.containsKey("AWS_ROLE_ARN")) {
            s3Props.put("AWS_ROLE_ARN", cosProps.get("COS_ROLE_ARN"));
        }
        s3Props.put("use_path_style", "false");
        return s3Props;
    }

    // -----------------------------------------------------------------------
    // Cloud-specific extension overrides
    // -----------------------------------------------------------------------

    /**
     * Generates a pre-signed PUT URL using the Tencent Cloud COS native SDK.
     *
     * @param objectKey the bare object key (no scheme or bucket prefix)
     */
    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = resolveRequired("COS_BUCKET", "AWS_BUCKET", "COS bucket for presigned URL");
        String region = resolveRequired("COS_REGION", "AWS_REGION", "COS region for presigned URL");
        try {
            COSClient cos = getCosClient(region);
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            URL url = cos.generatePresignedUrl(bucket, objectKey, expiration, HttpMethodName.PUT,
                    new java.util.HashMap<>(), new java.util.HashMap<>());
            LOG.info("Generated COS presigned URL for key={}", objectKey);
            return url.toString();
        } catch (CosClientException e) {
            LOG.warn("Failed to generate COS presigned URL for key={}", objectKey, e);
            throw new IOException("Failed to generate COS presigned URL: " + e.getMessage(), e);
        }
    }

    /**
     * Obtains temporary STS credentials using Tencent Cloud STS ({@code sts.tencentcloudapi.com}).
     */
    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("COS_REGION", "AWS_REGION", "COS region for STS");
        String accessKey = resolveRequired("COS_ACCESS_KEY", "AWS_ACCESS_KEY", "COS access key");
        String secretKey = resolveRequired("COS_SECRET_KEY", "AWS_SECRET_KEY", "COS secret key");
        String roleArn = resolveRequired("COS_ROLE_ARN", "AWS_ROLE_ARN", "COS role ARN");
        try {
            Credential credential = new Credential(accessKey, secretKey);
            StsClient stsClient = new StsClient(credential, region);
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = stsClient.AssumeRole(request);
            Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getTmpSecretId(),
                    credentials.getTmpSecretKey(),
                    credentials.getToken());
        } catch (Exception e) {
            LOG.warn("Failed to get COS STS token, roleArn={}", resolveOpt("COS_ROLE_ARN", "AWS_ROLE_ARN"), e);
            throw new IOException("Failed to get COS STS token: " + e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Native COS client lifecycle
    // -----------------------------------------------------------------------

    private COSClient getCosClient(String region) throws IOException {
        if (cosClient == null) {
            synchronized (this) {
                if (cosClient == null) {
                    cosClient = buildCosClient(region);
                }
            }
        }
        return cosClient;
    }

    protected COSClient buildCosClient(String region) throws IOException {
        String accessKey = resolveRequired("COS_ACCESS_KEY", "AWS_ACCESS_KEY", "COS access key");
        String secretKey = resolveRequired("COS_SECRET_KEY", "AWS_SECRET_KEY", "COS secret key");
        COSCredentials cred = new BasicCOSCredentials(accessKey, secretKey);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRegion(new Region(region));
        clientConfig.setHttpProtocol(HttpProtocol.https);
        return new COSClient(cred, clientConfig);
    }

    @Override
    public void close() throws IOException {
        if (cosClient != null) {
            cosClient.shutdown();
            cosClient = null;
        }
        super.close();
    }

    // -----------------------------------------------------------------------
    // Property helpers
    // -----------------------------------------------------------------------

    private String resolveOpt(String primaryKey, String fallbackKey) {
        String value = cosProperties.get(primaryKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return cosProperties.get(fallbackKey);
    }

    private String resolveRequired(String primaryKey, String fallbackKey, String description)
            throws IOException {
        String value = resolveOpt(primaryKey, fallbackKey);
        if (value == null || value.isEmpty()) {
            throw new IOException(description + " is required; set " + primaryKey + " in properties");
        }
        return value;
    }
}

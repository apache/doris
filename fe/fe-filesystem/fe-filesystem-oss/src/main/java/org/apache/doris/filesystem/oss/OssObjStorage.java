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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.BasicCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Alibaba Cloud OSS implementation of {@link org.apache.doris.filesystem.spi.ObjStorage}.
 *
 * <p>Extends {@link S3ObjStorage} so all core I/O operations (list, head, put, delete, copy,
 * multipart upload) delegate to the parent using S3-compatible APIs — matching the reference pattern
 * where {@code OssRemote extends DefaultRemote} (the S3-backed base class).
 *
 * <p>The two cloud-specific extension methods that <em>require</em> a native SDK are overridden:
 * <ul>
 *   <li>{@link #getPresignedUrl(String)} — uses Alibaba OSS SDK
 *       ({@code OSS.generatePresignedUrl}) to produce the correct OSS signature format.</li>
 *   <li>{@link #getStsToken()} — uses Alibaba RAM/STS SDK
 *       ({@code DefaultAcsClient}) to call {@code sts.aliyuncs.com}.</li>
 * </ul>
 *
 * <p>Recognized property keys (OSS-specific; AWS_* equivalents accepted as fallback):
 * <ul>
 *   <li>{@code OSS_ENDPOINT} / {@code AWS_ENDPOINT} — OSS endpoint URL</li>
 *   <li>{@code OSS_ACCESS_KEY} / {@code AWS_ACCESS_KEY} — access key ID</li>
 *   <li>{@code OSS_SECRET_KEY} / {@code AWS_SECRET_KEY} — secret access key</li>
 *   <li>{@code OSS_TOKEN} / {@code AWS_TOKEN} — STS session token (optional)</li>
 *   <li>{@code OSS_BUCKET} / {@code AWS_BUCKET} — bucket name (required for cloud extensions)</li>
 *   <li>{@code OSS_REGION} / {@code AWS_REGION} — region for STS calls</li>
 *   <li>{@code OSS_ROLE_ARN} / {@code AWS_ROLE_ARN} — role ARN for STS assumption</li>
 * </ul>
 */
public class OssObjStorage extends S3ObjStorage {

    private static final Logger LOG = LogManager.getLogger(OssObjStorage.class);

    /** Validity period for presigned URLs and STS tokens, in seconds. */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> ossProperties;
    private volatile OSS ossClient;

    public OssObjStorage(Map<String, String> properties) {
        super(toS3Props(properties));
        this.ossProperties = properties;
    }

    /**
     * Translates OSS-specific property keys to the AWS keys expected by {@link S3ObjStorage}.
     * If both forms are present, the AWS_* key takes precedence.
     */
    static Map<String, String> toS3Props(Map<String, String> ossProps) {
        Map<String, String> s3Props = new HashMap<>(ossProps);
        if (ossProps.containsKey("OSS_ENDPOINT") && !ossProps.containsKey("AWS_ENDPOINT")) {
            s3Props.put("AWS_ENDPOINT", ossProps.get("OSS_ENDPOINT"));
        }
        if (ossProps.containsKey("OSS_ACCESS_KEY") && !ossProps.containsKey("AWS_ACCESS_KEY")) {
            s3Props.put("AWS_ACCESS_KEY", ossProps.get("OSS_ACCESS_KEY"));
        }
        if (ossProps.containsKey("OSS_SECRET_KEY") && !ossProps.containsKey("AWS_SECRET_KEY")) {
            s3Props.put("AWS_SECRET_KEY", ossProps.get("OSS_SECRET_KEY"));
        }
        if (ossProps.containsKey("OSS_TOKEN") && !ossProps.containsKey("AWS_TOKEN")) {
            s3Props.put("AWS_TOKEN", ossProps.get("OSS_TOKEN"));
        }
        if (ossProps.containsKey("OSS_BUCKET") && !ossProps.containsKey("AWS_BUCKET")) {
            s3Props.put("AWS_BUCKET", ossProps.get("OSS_BUCKET"));
        }
        if (ossProps.containsKey("OSS_REGION") && !ossProps.containsKey("AWS_REGION")) {
            s3Props.put("AWS_REGION", ossProps.get("OSS_REGION"));
        }
        if (ossProps.containsKey("OSS_ROLE_ARN") && !ossProps.containsKey("AWS_ROLE_ARN")) {
            s3Props.put("AWS_ROLE_ARN", ossProps.get("OSS_ROLE_ARN"));
        }
        s3Props.put("use_path_style", "false");
        return s3Props;
    }

    // -----------------------------------------------------------------------
    // Cloud-specific extension overrides
    // -----------------------------------------------------------------------

    /**
     * Generates a pre-signed PUT URL using the Alibaba Cloud OSS native SDK.
     *
     * @param objectKey the bare object key (no scheme or bucket prefix)
     */
    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String bucket = resolveRequired("OSS_BUCKET", "AWS_BUCKET", "OSS bucket for presigned URL");
        try {
            OSS oss = getOssClient();
            Date expiration = new Date(System.currentTimeMillis() + (long) SESSION_EXPIRE_SECONDS * 1000);
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, objectKey, HttpMethod.PUT);
            request.setExpiration(expiration);
            URL signedUrl = oss.generatePresignedUrl(request);
            LOG.info("Generated OSS presigned URL for key={}", objectKey);
            return signedUrl.toString();
        } catch (OSSException e) {
            LOG.warn("Failed to generate OSS presigned URL for key={}", objectKey, e);
            throw new IOException("Failed to generate OSS presigned URL: " + e.getMessage(), e);
        }
    }

    /**
     * Obtains temporary STS credentials using Alibaba RAM STS ({@code sts.aliyuncs.com}).
     */
    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("OSS_REGION", "AWS_REGION", "OSS region for STS");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "AWS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "AWS_SECRET_KEY", "OSS secret key");
        String roleArn = resolveRequired("OSS_ROLE_ARN", "AWS_ROLE_ARN", "OSS role ARN");
        try {
            DefaultProfile profile = DefaultProfile.getProfile(region);
            BasicCredentials basicCredentials = new BasicCredentials(accessKey, secretKey);
            DefaultAcsClient ramClient =
                    new DefaultAcsClient(profile, new StaticCredentialsProvider(basicCredentials));
            AssumeRoleRequest request = new AssumeRoleRequest();
            request.setRoleArn(roleArn);
            request.setRoleSessionName("doris_" + UUID.randomUUID().toString().replace("-", ""));
            request.setDurationSeconds((long) SESSION_EXPIRE_SECONDS);
            AssumeRoleResponse response = ramClient.getAcsResponse(request);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            LOG.warn("Failed to get OSS STS token, roleArn={}", resolveOpt("OSS_ROLE_ARN", "AWS_ROLE_ARN"), e);
            throw new IOException("Failed to get OSS STS token: " + e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Native OSS client lifecycle
    // -----------------------------------------------------------------------

    private OSS getOssClient() throws IOException {
        if (ossClient == null) {
            synchronized (this) {
                if (ossClient == null) {
                    ossClient = buildOssClient();
                }
            }
        }
        return ossClient;
    }

    protected OSS buildOssClient() throws IOException {
        String endpoint = resolveRequired("OSS_ENDPOINT", "AWS_ENDPOINT", "OSS endpoint");
        String accessKey = resolveRequired("OSS_ACCESS_KEY", "AWS_ACCESS_KEY", "OSS access key");
        String secretKey = resolveRequired("OSS_SECRET_KEY", "AWS_SECRET_KEY", "OSS secret key");
        String token = resolveOpt("OSS_TOKEN", "AWS_TOKEN");
        if (token != null && !token.isEmpty()) {
            return new OSSClientBuilder().build(endpoint, accessKey, secretKey, token);
        }
        return new OSSClientBuilder().build(endpoint, accessKey, secretKey);
    }

    @Override
    public void close() throws IOException {
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
        super.close();
    }

    // -----------------------------------------------------------------------
    // Property helpers
    // -----------------------------------------------------------------------

    private String resolveOpt(String primaryKey, String fallbackKey) {
        String value = ossProperties.get(primaryKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return ossProperties.get(fallbackKey);
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

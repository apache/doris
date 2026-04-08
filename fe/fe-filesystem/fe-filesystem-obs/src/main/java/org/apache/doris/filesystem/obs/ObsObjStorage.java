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

package org.apache.doris.filesystem.obs;

import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.StsCredentials;

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.core.auth.ICredential;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuth;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuthIdentity;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityAssumerole;
import com.obs.services.ObsClient;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Huawei Cloud OBS implementation of {@link org.apache.doris.filesystem.spi.ObjStorage}.
 *
 * <p>Extends {@link S3ObjStorage} so all core I/O operations (list, head, put, delete, copy,
 * multipart upload) delegate to the parent using S3-compatible APIs — matching the reference pattern
 * where {@code ObsRemote extends DefaultRemote} (the S3-backed base class).
 *
 * <p>The two cloud-specific extension methods that <em>require</em> a native SDK are overridden:
 * <ul>
 *   <li>{@link #getPresignedUrl(String)} — uses Huawei OBS SDK
 *       ({@code ObsClient.createTemporarySignature}) to produce the correct OBS temporary URL.</li>
 *   <li>{@link #getStsToken()} — uses Huawei IAM SDK
 *       ({@code IamClient.createTemporaryAccessKeyByAgency}) to call the Huawei IAM endpoint.</li>
 * </ul>
 *
 * <p>Recognized property keys (OBS-specific; AWS_* equivalents accepted as fallback):
 * <ul>
 *   <li>{@code OBS_ENDPOINT} / {@code AWS_ENDPOINT} — OBS endpoint URL</li>
 *   <li>{@code OBS_ACCESS_KEY} / {@code AWS_ACCESS_KEY} — access key ID</li>
 *   <li>{@code OBS_SECRET_KEY} / {@code AWS_SECRET_KEY} — secret access key</li>
 *   <li>{@code OBS_BUCKET} / {@code AWS_BUCKET} — bucket name (required for cloud extensions)</li>
 *   <li>{@code OBS_REGION} / {@code AWS_REGION} — region (e.g. {@code cn-north-4})</li>
 *   <li>{@code OBS_AGENCY_NAME} — Huawei IAM agency name for STS (role name)</li>
 *   <li>{@code OBS_DOMAIN_NAME} — Huawei IAM domain name for STS (ARN equivalent)</li>
 * </ul>
 */
public class ObsObjStorage extends S3ObjStorage {

    private static final Logger LOG = LogManager.getLogger(ObsObjStorage.class);

    /** Validity period for presigned URLs and STS tokens, in seconds. */
    private static final int SESSION_EXPIRE_SECONDS = 3600;

    private final Map<String, String> obsProperties;

    public ObsObjStorage(Map<String, String> properties) {
        super(toS3Props(properties));
        this.obsProperties = properties;
    }

    /**
     * Translates OBS-specific property keys to the AWS keys expected by {@link S3ObjStorage}.
     * If both forms are present, the AWS_* key takes precedence.
     */
    static Map<String, String> toS3Props(Map<String, String> obsProps) {
        Map<String, String> s3Props = new HashMap<>(obsProps);
        if (obsProps.containsKey("OBS_ENDPOINT") && !obsProps.containsKey("AWS_ENDPOINT")) {
            s3Props.put("AWS_ENDPOINT", obsProps.get("OBS_ENDPOINT"));
        }
        if (obsProps.containsKey("OBS_ACCESS_KEY") && !obsProps.containsKey("AWS_ACCESS_KEY")) {
            s3Props.put("AWS_ACCESS_KEY", obsProps.get("OBS_ACCESS_KEY"));
        }
        if (obsProps.containsKey("OBS_SECRET_KEY") && !obsProps.containsKey("AWS_SECRET_KEY")) {
            s3Props.put("AWS_SECRET_KEY", obsProps.get("OBS_SECRET_KEY"));
        }
        if (obsProps.containsKey("OBS_BUCKET") && !obsProps.containsKey("AWS_BUCKET")) {
            s3Props.put("AWS_BUCKET", obsProps.get("OBS_BUCKET"));
        }
        if (obsProps.containsKey("OBS_REGION") && !obsProps.containsKey("AWS_REGION")) {
            s3Props.put("AWS_REGION", obsProps.get("OBS_REGION"));
        }
        s3Props.put("use_path_style", "false");
        return s3Props;
    }

    // -----------------------------------------------------------------------
    // Cloud-specific extension overrides
    // -----------------------------------------------------------------------

    /**
     * Generates a temporary signature URL for PUT using the Huawei OBS native SDK.
     *
     * @param objectKey the bare object key (no scheme or bucket prefix)
     */
    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        String endpoint = resolveRequired("OBS_ENDPOINT", "AWS_ENDPOINT", "OBS endpoint");
        String accessKey = resolveRequired("OBS_ACCESS_KEY", "AWS_ACCESS_KEY", "OBS access key");
        String secretKey = resolveRequired("OBS_SECRET_KEY", "AWS_SECRET_KEY", "OBS secret key");
        String bucket = resolveRequired("OBS_BUCKET", "AWS_BUCKET", "OBS bucket for presigned URL");
        ObsClient obsClient = buildObsClient(endpoint, accessKey, secretKey);
        try {
            TemporarySignatureRequest request = new TemporarySignatureRequest(
                    HttpMethodEnum.PUT, SESSION_EXPIRE_SECONDS);
            request.setBucketName(bucket);
            request.setObjectKey(objectKey);
            request.setHeaders(new HashMap<>());
            TemporarySignatureResponse response = obsClient.createTemporarySignature(request);
            String url = response.getSignedUrl();
            LOG.info("Generated OBS temporary signature URL for key={}", objectKey);
            return url;
        } finally {
            try {
                obsClient.close();
            } catch (IOException e) {
                LOG.warn("Failed to close ObsClient after presigned URL generation", e);
            }
        }
    }

    /**
     * Factory method for creating an {@link ObsClient}; protected for testability.
     */
    protected ObsClient buildObsClient(String endpoint, String accessKey, String secretKey) {
        return new ObsClient(accessKey, secretKey, endpoint);
    }

    /**
     * Obtains temporary STS credentials using Huawei IAM
     * ({@code createTemporaryAccessKeyByAgency}).
     */
    @Override
    public StsCredentials getStsToken() throws IOException {
        String region = resolveRequired("OBS_REGION", "AWS_REGION", "OBS region for STS");
        String accessKey = resolveRequired("OBS_ACCESS_KEY", "AWS_ACCESS_KEY", "OBS access key");
        String secretKey = resolveRequired("OBS_SECRET_KEY", "AWS_SECRET_KEY", "OBS secret key");
        String agencyName = resolveRequired("OBS_AGENCY_NAME", null, "OBS agency name for STS");
        String domainName = resolveRequired("OBS_DOMAIN_NAME", null, "OBS domain name for STS");
        try {
            ICredential auth = new GlobalCredentials().withAk(accessKey).withSk(secretKey);
            IamClient client = IamClient.newBuilder()
                    .withEndpoint("iam." + region + ".myhuaweicloud.com")
                    .withCredential(auth)
                    .build();
            IdentityAssumerole assumeRoleIdentity = new IdentityAssumerole();
            assumeRoleIdentity.withAgencyName(agencyName)
                    .withDomainName(domainName)
                    .withDurationSeconds(SESSION_EXPIRE_SECONDS);
            List<AgencyAuthIdentity.MethodsEnum> methods = new ArrayList<>();
            methods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role"));
            AgencyAuthIdentity identityAuth = new AgencyAuthIdentity();
            identityAuth.withMethods(methods).withAssumeRole(assumeRoleIdentity);
            AgencyAuth authBody = new AgencyAuth();
            authBody.withIdentity(identityAuth);
            CreateTemporaryAccessKeyByAgencyRequestBody body =
                    new CreateTemporaryAccessKeyByAgencyRequestBody().withAuth(authBody);
            CreateTemporaryAccessKeyByAgencyRequest request =
                    new CreateTemporaryAccessKeyByAgencyRequest().withBody(body);
            CreateTemporaryAccessKeyByAgencyResponse response =
                    client.createTemporaryAccessKeyByAgency(request);
            Credential credential = response.getCredential();
            return new StsCredentials(
                    credential.getAccess(),
                    credential.getSecret(),
                    credential.getSecuritytoken());
        } catch (Exception e) {
            LOG.warn("Failed to get OBS STS token, agencyName={}", agencyName, e);
            throw new IOException("Failed to get OBS STS token: " + e.getMessage(), e);
        }
    }

    // -----------------------------------------------------------------------
    // Property helpers
    // -----------------------------------------------------------------------

    private String resolveOpt(String primaryKey, String fallbackKey) {
        String value = obsProperties.get(primaryKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        if (fallbackKey != null) {
            return obsProperties.get(fallbackKey);
        }
        return null;
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

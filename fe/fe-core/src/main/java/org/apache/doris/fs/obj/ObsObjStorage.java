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

package org.apache.doris.fs.obj;

import org.apache.doris.cloud.storage.ObjectInfoAdapter;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.storage.OBSProperties;

import com.huaweicloud.sdk.iam.v3.model.AgencyAuth;
import com.huaweicloud.sdk.iam.v3.model.AgencyAuthIdentity;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByAgencyResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityAssumerole;
import com.obs.services.model.HttpMethodEnum;
import com.obs.services.model.TemporarySignatureRequest;
import com.obs.services.model.TemporarySignatureResponse;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Huawei Cloud OBS-specific {@link ObjStorage} implementation.
 *
 * <p>Inherits generic CRUD and {@code listObjectsWithPrefix} from {@link S3ObjStorage}
 * (OBS supports S3-compatible list API). Overrides STS (IAM agency) and
 * presigned URL (OBS TemporarySignature).
 *
 * <p>Note: in OBS, the {@code sts.role_arn} field carries the <em>domain name</em>
 * and {@code sts.role_name} carries the <em>agency name</em>.
 */
public class ObsObjStorage extends S3ObjStorage {
    private static final Logger LOG = LogManager.getLogger(ObsObjStorage.class);
    private static final long SESSION_EXPIRE_SECONDS = 3600L;

    private final OBSProperties obsProperties;

    public ObsObjStorage(OBSProperties properties) {
        super(properties);
        this.obsProperties = properties;
    }

    // ----------------------------------------------------------------
    // STS: Huawei Cloud IAM createTemporaryAccessKeyByAgency
    // (sts.role_name = agencyName, sts.role_arn = domainName)
    // ----------------------------------------------------------------

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        // OBS maps arn → domainName, roleName → agencyName
        String agencyName  = obsProperties.getOrigProps().get(ObjectInfoAdapter.STS_ROLE_NAME_KEY);
        String domainName  = obsProperties.getOrigProps().get(ObjectInfoAdapter.STS_ROLE_ARN_KEY);
        if (agencyName == null || domainName == null) {
            throw new DdlException("OBS STS requires sts.role_name (agency name) and sts.role_arn (domain name)");
        }
        try (ObsNativeClient nativeClient = new ObsNativeClient(obsProperties)) {
            IdentityAssumerole assumerole = new IdentityAssumerole();
            assumerole.withAgencyName(agencyName)
                    .withDomainName(domainName)
                    .withDurationSeconds(ObjectInfoAdapter.getDurationSeconds());
            List<AgencyAuthIdentity.MethodsEnum> methods = new ArrayList<>();
            methods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role"));
            AgencyAuthIdentity identity = new AgencyAuthIdentity();
            identity.withMethods(methods).withAssumeRole(assumerole);
            AgencyAuth auth = new AgencyAuth().withIdentity(identity);
            CreateTemporaryAccessKeyByAgencyRequestBody body =
                    new CreateTemporaryAccessKeyByAgencyRequestBody().withAuth(auth);
            CreateTemporaryAccessKeyByAgencyRequest req =
                    new CreateTemporaryAccessKeyByAgencyRequest().withBody(body);
            CreateTemporaryAccessKeyByAgencyResponse resp =
                    nativeClient.getIamClient().createTemporaryAccessKeyByAgency(req);
            Credential cred = resp.getCredential();
            return Triple.of(cred.getAccess(), cred.getSecret(), cred.getSecuritytoken());
        } catch (Throwable e) {
            LOG.warn("Failed to get OBS STS token", e);
            throw new DdlException("Failed to get OBS STS token: " + e.getMessage());
        }
    }

    // ----------------------------------------------------------------
    // Presigned URL: OBS TemporarySignature
    // ----------------------------------------------------------------

    @Override
    public String getPresignedUrl(String objectKey) throws IOException {
        try (ObsNativeClient nativeClient = new ObsNativeClient(obsProperties)) {
            TemporarySignatureRequest req = new TemporarySignatureRequest(
                    HttpMethodEnum.PUT, SESSION_EXPIRE_SECONDS);
            req.setBucketName(obsProperties.getBucket());
            req.setObjectKey(objectKey);
            req.setHeaders(new HashMap<>());
            TemporarySignatureResponse resp =
                    nativeClient.getObsClient().createTemporarySignature(req);
            String url = resp.getSignedUrl();
            LOG.info("Generated OBS presigned URL for key={}", objectKey);
            return url;
        } catch (Throwable e) {
            throw new IOException("Failed to generate OBS presigned URL: " + e.getMessage(), e);
        }
    }

    // listObjectsWithPrefix inherits S3ObjStorage (OBS supports S3-compatible list API)
}

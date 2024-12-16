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

import org.apache.doris.common.DdlException;

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
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ObsRemote extends DefaultRemote {
    private static final Logger LOG = LogManager.getLogger(ObsRemote.class);

    public ObsRemote(ObjectInfo obj) {
        super(obj);
    }

    @Override
    public String getPresignedUrl(String fileName) {
        String endPoint = obj.getEndpoint();
        String ak = obj.getAk();
        String sk = obj.getSk();

        ObsClient obsClient = new ObsClient(ak, sk, endPoint);
        TemporarySignatureRequest request = new TemporarySignatureRequest(
                HttpMethodEnum.PUT, SESSION_EXPIRE_SECOND);
        request.setBucketName(obj.getBucket());
        request.setObjectKey(normalizePrefix(fileName));
        request.setHeaders(new HashMap<String, String>());

        TemporarySignatureResponse response = obsClient.createTemporarySignature(request);

        String url = response.getSignedUrl();
        LOG.info("obs temporary signature url: {}", url);
        return url;
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        ICredential auth = new GlobalCredentials().withAk(obj.getAk()).withSk(obj.getSk());
        IamClient client = IamClient.newBuilder().withEndpoint("iam." + obj.getRegion() + ".myhuaweicloud.com")
                .withCredential(auth)/*.withRegion(IamRegion.valueOf(obj.getRegion()))*/.build();
        CreateTemporaryAccessKeyByAgencyRequestBody body = new CreateTemporaryAccessKeyByAgencyRequestBody();
        IdentityAssumerole assumeRoleIdentity = new IdentityAssumerole();
        assumeRoleIdentity.withAgencyName(obj.getRoleName()).withDomainName(obj.getArn())
                .withDurationSeconds(getDurationSeconds());
        List<AgencyAuthIdentity.MethodsEnum> listIdentityMethods = new ArrayList<>();
        listIdentityMethods.add(AgencyAuthIdentity.MethodsEnum.fromValue("assume_role"));
        AgencyAuthIdentity identityAuth = new AgencyAuthIdentity();
        identityAuth.withMethods(listIdentityMethods).withAssumeRole(assumeRoleIdentity);
        AgencyAuth authbody = new AgencyAuth();
        authbody.withIdentity(identityAuth);
        body.withAuth(authbody);
        CreateTemporaryAccessKeyByAgencyRequest request = new CreateTemporaryAccessKeyByAgencyRequest();
        request.withBody(body);
        try {
            CreateTemporaryAccessKeyByAgencyResponse response = client.createTemporaryAccessKeyByAgency(request);
            Credential credential = response.getCredential();
            return Triple.of(credential.getAccess(), credential.getSecret(),
                    credential.getSecuritytoken());
        } catch (Throwable e) {
            LOG.warn("Failed get obs sts token", e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "ObsRemote{obj=" + obj + '}';
    }
}

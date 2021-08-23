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

package org.apache.doris.stack.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.IdaasSettingReq;
import org.apache.doris.stack.model.response.config.IdaasResult;
import org.apache.doris.stack.model.response.config.IdaasSettingResp;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.service.config.ConfigConstant;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

@Component
@Slf4j
public class IdaasComponent {

    @Autowired
    private SettingComponent settingComponent;

    private final Base64.Encoder encoder = Base64.getEncoder();
    public boolean checkIdaasConnection(IdaasSettingReq idaasSettingReq) throws Exception {
        try {
            getAccessToken(idaasSettingReq);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public ResponseEntity<IdaasResult> getAccessToken(IdaasSettingReq idaasSettingReq) throws Exception {
        String getAccessTokenURL =
                idaasSettingReq.getIdaasProjectURL() + "/app/oauth/" + idaasSettingReq.getIdaasAppId() + "/token";
        RestTemplate restTemplate = new RestTemplate();
        // Set request parameters form-data
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("grant_type", "client_credentials");
        map.add("scope", "admin");
        HttpHeaders headers = new HttpHeaders();
        // Set authentication string
        String basicAuth = encoder.encodeToString((idaasSettingReq.getIdaasClientId() + ":" + idaasSettingReq.getIdaasClientSecret()).getBytes(
                StandardCharsets.UTF_8));
        log.debug("idaas base64 is {}.", basicAuth);
        headers.setBasicAuth(basicAuth);
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(map, headers);
        ResponseEntity<IdaasResult> responseEntity = restTemplate.postForEntity(getAccessTokenURL, request,
                IdaasResult.class);
        return responseEntity;
    }

    public void addIdaasConfig(IdaasSettingReq idaasSettingReq, Map<String, String> configCache) throws Exception {
        settingComponent.addNewSetting(ConfigConstant.IDAAS_PROJECT_URL_KEY, idaasSettingReq.getIdaasProjectURL(),
                configCache);

        settingComponent.addNewSetting(ConfigConstant.IDAAS_APP_ID_KEY, idaasSettingReq.getIdaasAppId(),
                configCache);

        settingComponent.addNewSetting(ConfigConstant.IDAAS_PROJECT_ID_KEY, idaasSettingReq.getIdaasProjectId(),
                configCache);

        settingComponent.addNewSetting(ConfigConstant.IDAAS_CLIENT_ID_KEY, idaasSettingReq.getIdaasClientId(),
                configCache);

        settingComponent.addNewSetting(ConfigConstant.IDAAS_CLIENT_SECRET_KEY, idaasSettingReq.getIdaasClientSecret(),
                configCache);
    }

    /**
     * Is Idaas authentication enabled
     *
     * @return
     */
    public boolean enabled() {
        SettingEntity entity = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);

        if (entity == null) {
            log.debug("The idaas is not enabled");
            return false;
        } else if (entity.getValue().equals(InitStudioReq.AuthType.idaas.name())) {
            log.debug("The idaas is enabled");
            return true;
        } else {
            return false;
        }
    }

    /**
     * get Idaas Configuration item
     *
     * @return
     */
    public IdaasSettingResp readIdaasConfig() {
        IdaasSettingResp idaasSettingResp = new IdaasSettingResp();

        if (!enabled()) {
            log.warn("Idaas configuration don't exist.");
            idaasSettingResp.setIdaasEnabled(false);
            return idaasSettingResp;
        }

        idaasSettingResp.setIdaasEnabled(true);

        SettingEntity idaasProjectURL = settingComponent.readSetting(ConfigConstant.IDAAS_PROJECT_URL_KEY);
        if (idaasProjectURL != null) {
            idaasSettingResp.setIdaasProjectURL(idaasProjectURL.getValue());
        }

        SettingEntity idaasProjectId = settingComponent.readSetting(ConfigConstant.IDAAS_PROJECT_ID_KEY);
        if (idaasProjectId != null) {
            idaasSettingResp.setIdaasProjectId(idaasProjectId.getValue());
        }

        SettingEntity idaasAppId = settingComponent.readSetting(ConfigConstant.IDAAS_APP_ID_KEY);
        if (idaasAppId != null) {
            idaasSettingResp.setIdaasAppId(idaasAppId.getValue());
        }

        SettingEntity idaasClientId = settingComponent.readSetting(ConfigConstant.IDAAS_CLIENT_ID_KEY);
        if (idaasClientId != null) {
            idaasSettingResp.setIdaasClientId(idaasClientId.getValue());
        }

        SettingEntity idaasClientSecret = settingComponent.readSetting(ConfigConstant.IDAAS_CLIENT_SECRET_KEY);
        if (idaasClientSecret != null) {
            idaasSettingResp.setIdaasClientSecret(idaasClientSecret.getValue());
        }

        return idaasSettingResp;
    }

}

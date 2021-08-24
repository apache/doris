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

package org.apache.doris.stack.model.request.config;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class IdaasSettingReq {

    @JSONField(name = "idaas-project-url")
    @JsonProperty("idaas-project-url")
    private String idaasProjectURL;

    @JSONField(name = "idaas-project-id")
    @JsonProperty("idaas-project-id")
    private String idaasProjectId;

    @JSONField(name = "idaas-app-id")
    @JsonProperty("idaas-app-id")
    private String idaasAppId;

    @JSONField(name = "idaas-client-id")
    @JsonProperty("idaas-client-id")
    private String idaasClientId;

    @JSONField(name = "idaas-client-secret")
    @JsonProperty("idaas-client-secret")
    private String idaasClientSecret;

    public boolean hasEmptyField() {

        return idaasProjectURL == null || idaasProjectId == null || idaasAppId == null || idaasClientId == null
                || idaasClientSecret == null;
    }
}

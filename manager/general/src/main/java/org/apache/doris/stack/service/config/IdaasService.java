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

package org.apache.doris.stack.service.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.apache.doris.stack.model.request.config.IdaasSettingReq;
import org.apache.doris.stack.model.response.config.IdaasSettingResp;
import org.apache.doris.stack.component.IdaasComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.exception.IdaasConnectionException;
import org.apache.doris.stack.exception.IdaasNotExistException;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: Idaas Service
 **/
@Service
@Slf4j
public class IdaasService extends BaseService {

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private IdaasComponent idaasComponent;

    /**
     * get idaas information
     * @return
     */
    public IdaasSettingResp setting() {
        log.debug("Get idaas setting info.");
        return idaasComponent.readIdaasConfig();
    }

    @Transactional
    public void update(IdaasSettingReq idaasSettingReq) throws Exception {

        if (!idaasComponent.enabled()) {
            log.error("The idaas config not exist.");
            throw new IdaasNotExistException();
        }

        // Check whether the request information is correct
        checkRequestBody(idaasSettingReq.hasEmptyField());

        // test idaas connectivity
        // get idaas configuration items
        boolean isConnection = idaasComponent.checkIdaasConnection(idaasSettingReq);
        if (!isConnection) {
            throw new IdaasConnectionException();
        }

        log.debug("update idaas config.");

        // Store the cache information and update the cache in batches after writing to the database successfully,
        // because the database can be rolled back, but the cache cannot
        Map<String, String> configCache = new HashMap<>();

        idaasComponent.addIdaasConfig(idaasSettingReq, configCache);

        ConfigCache.writeConfigs(configCache);
        log.debug("Update idaas config success.");
    }

}

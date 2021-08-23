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

import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.model.response.config.LdapSettingResp;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.exception.LdapConnectionException;
import org.apache.doris.stack.exception.LdapNotExistException;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description: Ldap Service
 **/
@Service
@Slf4j
public class LdapService extends BaseService {

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private LdapComponent ldapComponent;

    /**
     * get Ldap information
     * @return
     */
    public LdapSettingResp setting() {
        log.debug("Get ldap setting info.");
        return ldapComponent.readLdapConfig();
    }

    @Transactional
    public void update(LdapSettingReq ldapSettingReq) throws Exception {

        if (!ldapComponent.enabled()) {
            log.error("The ldap config not exist.");
            throw new LdapNotExistException();
        }

        // check request
        checkRequestBody(ldapSettingReq.hasEmptyField());

        // test ldap connectivity
        // Get all LDAP configuration items
        boolean isConnection = ldapComponent.checkLdapConnection(ldapSettingReq);
        if (!isConnection) {
            throw new LdapConnectionException();
        }

        log.debug("update ldap config.");

        // Store the cache information and update the cache in batches after writing to the database successfully,
        // because the database can be rolled back, but the cache cannot
        Map<String, String> configCache = new HashMap<>();

        ldapComponent.addLdapConfig(ldapSettingReq, configCache);

        ConfigCache.writeConfigs(configCache);
        log.debug("Update ldap config success.");
    }

}

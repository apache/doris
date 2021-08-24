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

import org.apache.doris.stack.model.request.config.EmailInfo;
import org.apache.doris.stack.model.request.config.TestEmailReq;
import org.apache.doris.stack.model.response.config.SettingItem;
import org.apache.doris.stack.component.MailComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.service.BaseService;
import org.apache.doris.stack.service.UtilService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description：Mailbox configuration implementation
 */
@Service
@Slf4j
public class EmailService extends BaseService {

    private MailComponent mailComponent;

    private UtilService utilService;

    private SettingComponent settingComponent;

    @Autowired
    public EmailService(MailComponent mailComponent,
                        UtilService utilService,
                        SettingComponent settingComponent) {
        this.mailComponent = mailComponent;
        this.utilService = utilService;
        this.settingComponent = settingComponent;
    }

    /**
     * update
     * @param emailInfo
     * @throws Exception
     */
    @Transactional
    public void updateEmailInfo(EmailInfo emailInfo) throws Exception {
        log.debug("update email config.");
        checkRequestBody(emailInfo.hasEmptyField());

        // Check whether the current email address meets the requirements
        utilService.emailCheck(emailInfo.getFromAddress());

        // Check whether the current cluster has been configured
        boolean configured = mailComponent.checkConfigExist();

        // Store cache information
        Map<String, String> configCache = new HashMap<>();

        if (!configured) {
            log.debug("Add new mail config.");
            // Mail has not been configured before. Add mail directly
            // Store in database and cache
            settingComponent.addNewSetting(ConfigConstant.EMAIL_CONFIGURED_KEY, "true", configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_HOST_KEY, emailInfo.getSmtpHost(), configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_PORT_KEY, emailInfo.getSmtpPort(), configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_SECURITY_KEY, emailInfo.getSmtpSecurity(), configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_ADDRESS_KEY, emailInfo.getFromAddress(), configCache);

            if (!emailInfo.getSmtpSecurity().equals(EmailInfo.SmtpSecurity.none.name())) {
                settingComponent.addNewSetting(ConfigConstant.EMAIL_USER_NAME_KEY, emailInfo.getSmtpUsername(), configCache);

                settingComponent.addNewSetting(ConfigConstant.EMAIL_PASSWORD_KEY, emailInfo.getSmtpPassword(), configCache);
            }
        } else {
            log.debug("update mail config.");
            // Mail has been configured before, and mail information has been modified
            settingComponent.addNewSetting(ConfigConstant.EMAIL_HOST_KEY, emailInfo.getSmtpHost(), configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_PORT_KEY, emailInfo.getSmtpPort(), configCache);

            SettingEntity oldSecurity = settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY);
            settingComponent.addNewSetting(ConfigConstant.EMAIL_SECURITY_KEY, emailInfo.getSmtpSecurity(), configCache);

            settingComponent.addNewSetting(ConfigConstant.EMAIL_ADDRESS_KEY, emailInfo.getFromAddress(), configCache);

            // The two security protocols are different
            if (oldSecurity != null && oldSecurity.getValue() != null
                    && !oldSecurity.getValue().equals(emailInfo.getSmtpSecurity())) {
                log.debug("security protocol changed.");
                // If the new SMTP security protocol is none, delete the original user name and password
                if (emailInfo.getSmtpSecurity().equals(EmailInfo.SmtpSecurity.none.name())) {
                    log.debug("delete user name and password.");
                    settingComponent.deleteSetting(ConfigConstant.EMAIL_USER_NAME_KEY);
                    settingComponent.deleteSetting(ConfigConstant.EMAIL_PASSWORD_KEY);

                    ConfigCache.writeConfigs(configCache);
                    return;
                }
            } else {
                log.debug("security protocol not changed.");
                if (oldSecurity.getValue().equals(EmailInfo.SmtpSecurity.none.name())) {
                    log.debug("security protocol is none, no need update user and password.");
                    ConfigCache.writeConfigs(configCache);
                    return;
                }
            }

            // If the previous SMTP security protocol is none, add a user name and password.
            // If the security protocol has not been changed and is not none, modify the user name and password
            log.debug("update user and password");
            settingComponent.addNewSetting(ConfigConstant.EMAIL_USER_NAME_KEY, emailInfo.getSmtpUsername(), configCache);
            settingComponent.addNewSetting(ConfigConstant.EMAIL_PASSWORD_KEY, emailInfo.getSmtpPassword(), configCache);

            ConfigCache.writeConfigs(configCache);
        }
    }

    @Transactional
    public void deleteEmailInfo() {
        log.debug("Super admin User delete email config.");

        boolean configured = mailComponent.checkConfigExist();

        if (configured) {
            log.debug("delete email config.");
            settingComponent.deleteSetting(ConfigConstant.EMAIL_CONFIGURED_KEY);
            settingComponent.deleteSetting(ConfigConstant.EMAIL_HOST_KEY);
            settingComponent.deleteSetting(ConfigConstant.EMAIL_PORT_KEY);
            settingComponent.deleteSetting(ConfigConstant.EMAIL_ADDRESS_KEY);

            SettingEntity securityConfig = settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY);
            settingComponent.deleteSetting(ConfigConstant.EMAIL_SECURITY_KEY);

            if (securityConfig != null && securityConfig.getValue() != null
                    && !securityConfig.getValue().equals(EmailInfo.SmtpSecurity.none.name())) {
                settingComponent.deleteSetting(ConfigConstant.EMAIL_USER_NAME_KEY);
                settingComponent.deleteSetting(ConfigConstant.EMAIL_PASSWORD_KEY);
            }
        }
        log.debug("Delete sucess.");
    }

    public void sendTestEmail(TestEmailReq testEmailReq) throws Exception {
        log.debug("Super User send test mail by:{}.", testEmailReq.getEmail());
        utilService.emailCheck(testEmailReq.getEmail());
        mailComponent.sendTestEmail(testEmailReq.getEmail());
        log.debug("Send test mail success.");
    }

    public List<SettingItem> getEmailConfig(int userId) {
        log.debug("User {} get email config.", userId);
        return settingComponent.getUserEmailConfig();
    }
}

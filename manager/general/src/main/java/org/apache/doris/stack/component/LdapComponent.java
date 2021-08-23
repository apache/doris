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

import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfoReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.LdapSettingReq;
import org.apache.doris.stack.model.response.config.LdapSettingResp;
import org.apache.doris.stack.connector.LdapClient;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.UserLoginException;
import org.apache.doris.stack.service.config.ConfigConstant;
import com.unboundid.ldap.sdk.LDAPConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class LdapComponent {
    private static final String USER_BASE_DIV = "&";

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private LdapClient ldapClient;

    public void addLdapConfig(LdapSettingReq ldapSettingReq, Map<String, String> configCache) throws Exception {

        settingComponent.addNewSetting(ConfigConstant.LDAP_HOST_KEY, ldapSettingReq.getLdapHost(), configCache);

        String portStr;
        if (ldapSettingReq.getLdapPort() == null) {
            portStr = LdapSettingReq.PORT;
        } else {
            portStr = ldapSettingReq.getLdapPort().toString();
        }
        settingComponent.addNewSetting(ConfigConstant.LDAP_PORT_KEY, portStr, configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_SECURITY_KEY, ldapSettingReq.getLdapSecurity(), configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_BIND_DN_KEY, ldapSettingReq.getLdapBindDn(), configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_PASSWORD_KEY, ldapSettingReq.getLdapPassword(), configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_USER_BASE_KEY,
                String.join(USER_BASE_DIV, ldapSettingReq.getLdapUserBase()), configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_USER_FILTER_KEY, ldapSettingReq.getLdapUserFilter(), configCache);

        // The default setting property is mail
        String ldapAttributeEmailStr;
        if (ldapSettingReq.getLdapAttributeEmail() == null) {
            ldapAttributeEmailStr = LdapSettingReq.MAIL;
        } else {
            ldapAttributeEmailStr = ldapSettingReq.getLdapAttributeEmail();
        }

        settingComponent.addNewSetting(ConfigConstant.LDAP_ATTRIBUTE_EMAIL_KEY, ldapAttributeEmailStr, configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_ATTRIBUTE_FIRSTNAME_KEY, ldapSettingReq.getLdapAttributeFirstName(), configCache);

        settingComponent.addNewSetting(ConfigConstant.LDAP_ATTRIBUTE_LASTNAME_KEY, ldapSettingReq.getLdapAttributeLastName(), configCache);
    }

    /**
     * Is LDAP authentication enabled
     *
     * @return
     */
    public boolean enabled() {
        SettingEntity entity = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);

        if (entity == null) {
            log.debug("The ldap is not enabled");
            return false;
        } else if (entity.getValue().equals(InitStudioReq.AuthType.ldap.name())) {
            log.debug("The ldap is enabled");
            return true;
        } else {
            return false;
        }
    }

    /**
     * get LDAP Configuration item
     *
     * @return
     */
    public LdapSettingResp readLdapConfig() {
        LdapSettingResp ldapSettingResp = new LdapSettingResp();

        if (!enabled()) {
            log.warn("Ldap configuration don't exist.");
            ldapSettingResp.setLdapEnabled(false);
            return ldapSettingResp;
        }

        ldapSettingResp.setLdapEnabled(true);

        SettingEntity ldapHost = settingComponent.readSetting(ConfigConstant.LDAP_HOST_KEY);
        if (ldapHost != null) {
            ldapSettingResp.setLdapHost(ldapHost.getValue());
        }

        SettingEntity ldapPort = settingComponent.readSetting(ConfigConstant.LDAP_PORT_KEY);
        if (ldapPort != null) {
            ldapSettingResp.setLdapPort(Integer.parseInt(ldapPort.getValue()));
        }

        SettingEntity ldapSecurity = settingComponent.readSetting(ConfigConstant.LDAP_SECURITY_KEY);
        if (ldapSecurity != null) {
            ldapSettingResp.setLdapSecurity(ldapSecurity.getValue());
        }

        SettingEntity ldapBindDN = settingComponent.readSetting(ConfigConstant.LDAP_BIND_DN_KEY);
        if (ldapBindDN != null) {
            ldapSettingResp.setLdapBindDn(ldapBindDN.getValue());
        }

        SettingEntity ldapPassword = settingComponent.readSetting(ConfigConstant.LDAP_PASSWORD_KEY);
        if (ldapPassword != null) {
            ldapSettingResp.setLdapPassword(ldapPassword.getValue());
        }

        ldapSettingResp.setLdapUserBase(getBaseDn());

        SettingEntity ldapUserFilter = settingComponent.readSetting(ConfigConstant.LDAP_USER_FILTER_KEY);
        if (ldapUserFilter != null) {
            ldapSettingResp.setLdapUserFilter(ldapUserFilter.getValue());
        }

        SettingEntity ldapAttributeEmail = settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_EMAIL_KEY);
        if (ldapAttributeEmail != null) {
            ldapSettingResp.setLdapAttributeEmail(ldapAttributeEmail.getValue());
        }

        SettingEntity ldapAttributeFirstName = settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_FIRSTNAME_KEY);
        if (ldapAttributeFirstName != null) {
            ldapSettingResp.setLdapAttributeFirstName(ldapAttributeFirstName.getValue());
        }

        SettingEntity ldapAttributeLastName = settingComponent.readSetting(ConfigConstant.LDAP_ATTRIBUTE_LASTNAME_KEY);
        if (ldapAttributeLastName != null) {
            ldapSettingResp.setLdapAttributeLastName(ldapAttributeLastName.getValue());
        }

        return ldapSettingResp;
    }

    /**
     * get Base DN
     *
     * @return
     */
    public List<String> getBaseDn() {
        SettingEntity userBases = settingComponent.readSetting(ConfigConstant.LDAP_USER_BASE_KEY);
        if (userBases != null && userBases.getValue() != null) {
            return Arrays.asList(userBases.getValue().split(USER_BASE_DIV));
        } else {
            return null;
        }
    }

    /**
     * get ldap Connected entity
     *
     * @return
     */
    public LdapConnectionInfo getConnInfo() {
        LdapSettingResp resp = readLdapConfig();

        LdapConnectionInfo ldapConnection = new LdapConnectionInfo();
        ldapConnection.setHost(resp.getLdapHost());
        ldapConnection.setPort(resp.getLdapPort());
        ldapConnection.setBindDn(resp.getLdapBindDn());
        ldapConnection.setPassword(resp.getLdapPassword());
        ldapConnection.setAttributeEmail(resp.getLdapAttributeEmail());

        return ldapConnection;
    }

    /**
     * Encapsulate LDAP request information
     *
     * @param ldapSettingReq
     * @return
     */
    public boolean checkLdapConnection(LdapSettingReq ldapSettingReq) {
        log.debug("check ldap connection info by LdapSettingReq.");
        LdapConnectionInfo ldapConnection = new LdapConnectionInfo();
        ldapConnection.setHost(ldapSettingReq.getLdapHost());
        if (ldapSettingReq.getLdapPort() == null) {
            ldapConnection.setPort(Integer.valueOf(LdapSettingReq.PORT));
        } else {
            ldapConnection.setPort(ldapSettingReq.getLdapPort());
        }
        ldapConnection.setBindDn(ldapSettingReq.getLdapBindDn());
        ldapConnection.setPassword(ldapSettingReq.getLdapPassword());
        ldapConnection.setSecurity(ldapSettingReq.getLdapSecurity());
        ldapConnection.setUserBase(ldapSettingReq.getLdapUserBase());
        ldapConnection.setUserFilter(ldapSettingReq.getLdapUserFilter());
        if (ldapSettingReq.getLdapAttributeEmail() == null) {
            ldapConnection.setAttributeEmail(LdapSettingReq.MAIL);
        } else {
            ldapConnection.setAttributeEmail(ldapSettingReq.getLdapAttributeEmail());
        }

        ldapConnection.setAttributeFirstname(ldapSettingReq.getLdapAttributeFirstName());
        ldapConnection.setAttributeLastname(ldapSettingReq.getLdapAttributeLastName());

        if (ldapClient.getConnection(ldapConnection) == null) {
            log.error("Ldap Connection info error, please check.");
            return false;
        }
        log.debug("Ldap Connection success.");
        return true;
    }

    /**
     * Find users by mailbox ID
     *
     * @param email
     * @return
     */
    public LdapUserInfo searchUserByEmail(String email) throws Exception {
        // get LDAP connection
        LdapConnectionInfo connectionInfo = getConnInfo();
        LDAPConnection connection = ldapClient.getConnection(connectionInfo);

        System.out.println(connection);

        // LDAP request
        LdapUserInfoReq userInfoReq = new LdapUserInfoReq();
        userInfoReq.setBaseDn(getBaseDn());
        userInfoReq.setUserAttribute(connectionInfo.getAttributeEmail());
        userInfoReq.setUserValue(email);

        // search user
        LdapUserInfo ldapUser = ldapClient.getUser(connection, userInfoReq);
        if (ldapUser.getExist()) {
            return ldapUser;
        }
        log.error("Ldap User {} is not exist, please check and try again.", email);
        throw new UserLoginException();
    }
}

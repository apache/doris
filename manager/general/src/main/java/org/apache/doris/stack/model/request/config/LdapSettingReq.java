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

import java.util.List;

/**
 * @Description: Ldap setting request
 **/
@Data
public class LdapSettingReq {

    public static final String MAIL = "mail";

    public static final String PORT = "389";

    @JSONField(name = "ldap-attribute-email")
    @JsonProperty("ldap-attribute-email")
    private String ldapAttributeEmail = "mail";

    @JSONField(name = "ldap-attribute-firstname")
    @JsonProperty("ldap-attribute-firstname")
    private String ldapAttributeFirstName;

    @JSONField(name = "ldap-attribute-lastname")
    @JsonProperty("ldap-attribute-lastname")
    private String ldapAttributeLastName;

    @JSONField(name = "ldap-bind-dn")
    @JsonProperty("ldap-bind-dn")
    private String ldapBindDn;

    @JSONField(name = "ldap-enabled")
    @JsonProperty("ldap-enabled")
    private Boolean ldapEnabled;

    @JSONField(name = "ldap-group-base")
    @JsonProperty("ldap-group-base")
    private String ldapGroupBase;

    @JSONField(name = "ldap-group-mappings")
    @JsonProperty("ldap-group-mappings")
    private Object ldapGroupMappings;

    @JSONField(name = "ldap-group-sync")
    @JsonProperty("ldap-group-sync")
    private Boolean ldapGroupSync;

    @JSONField(name = "ldap-host")
    @JsonProperty("ldap-host")
    private String ldapHost;

    @JSONField(name = "ldap-password")
    @JsonProperty("ldap-password")
    private String ldapPassword;

    @JSONField(name = "ldap-port")
    @JsonProperty("ldap-port")
    private Integer ldapPort = 389;

    @JSONField(name = "ldap-security")
    @JsonProperty("ldap-security")
    private String ldapSecurity;

    @JSONField(name = "ldap-user-base")
    @JsonProperty("ldap-user-base")
    private List<String> ldapUserBase;

    @JSONField(name = "ldap-user-filter")
    @JsonProperty("ldap-user-filter")
    private String ldapUserFilter;

    /**
     * check empty field
     *
     * @return boolean
     */
    public boolean hasEmptyField() {
        if (ldapHost == null || ldapBindDn == null || ldapPassword == null || ldapUserBase == null
                || ldapUserBase.isEmpty()) {
            return true;
        }

        return false;
    }
}

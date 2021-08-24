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

package org.apache.doris.stack.model.response.config;

import lombok.Data;

import java.util.List;

/**
 * @Description: Ldap setting  response
 **/
@Data
public class LdapSettingResp {

    private String ldapAttributeEmail;

    private String ldapAttributeFirstName;

    private String ldapAttributeLastName;

    private String ldapBindDn;

    private Boolean ldapEnabled;

    private String ldapGroupBase;

    private Object ldapGroupMappings;

    private Boolean ldapGroupSync;

    private String ldapHost;

    private String ldapPassword;

    private Integer ldapPort;

    private String ldapSecurity;

    private List<String> ldapUserBase;

    private String ldapUserFilter;
}

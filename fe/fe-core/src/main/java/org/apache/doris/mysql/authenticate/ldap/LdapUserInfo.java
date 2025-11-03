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

package org.apache.doris.mysql.authenticate.ldap;

import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.Role;

import java.util.Objects;
import java.util.Set;

/**
 * Used to cache LDAP information of user, such as password and privileges.
 */
public class LdapUserInfo {
    public LdapUserInfo(String userName, boolean isSetPasswd, String passwd, Set<Role> roles) {
        this.userName = userName;
        this.isExists = true;
        this.isSetPasswd = isSetPasswd;
        this.passwd = passwd;
        this.roles = roles;
        this.lastTimeStamp = System.currentTimeMillis();
    }

    private LdapUserInfo(String userName, boolean isSetPasswd, String passwd, Set<Role> roles, long lastTimeStamp) {
        this.userName = userName;
        this.isExists = true;
        this.isSetPasswd = isSetPasswd;
        this.passwd = passwd;
        this.roles = roles;
        this.lastTimeStamp = lastTimeStamp;
    }

    public LdapUserInfo(String notExistsUserName) {
        this.userName = notExistsUserName;
        this.isExists = false;
        this.isSetPasswd = false;
        this.passwd = null;
        this.roles = null;
        this.lastTimeStamp = System.currentTimeMillis();
    }

    private final String userName;

    private final boolean isExists;

    private final boolean isSetPasswd;

    private final String passwd;

    private final Set<Role> roles;

    private final long lastTimeStamp;

    public String getUserName() {
        return userName;
    }

    // The password needs to be checked by LdapManager for updated cache, so it is visible in the package.
    public boolean isSetPasswd() {
        return isSetPasswd;
    }

    String getPasswd() {
        return passwd;
    }

    public Set<Role> getRoles() {
        return roles;
    }

    public boolean isExists() {
        return isExists;
    }

    public LdapUserInfo cloneWithPasswd(String passwd) {
        if (Objects.isNull(passwd)) {
            return new LdapUserInfo(userName, isSetPasswd, this.passwd, roles, lastTimeStamp);
        }

        return new LdapUserInfo(userName, true, passwd, roles, lastTimeStamp);
    }

    // Return true if LdapUserInfo is exceeded the time limit;
    public boolean checkTimeout() {
        return System.currentTimeMillis() > lastTimeStamp + LdapConfig.ldap_user_cache_timeout_s * 1000;
    }
}

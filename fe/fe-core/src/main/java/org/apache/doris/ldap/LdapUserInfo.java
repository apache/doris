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

package org.apache.doris.ldap;

import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.PaloRole;

import java.util.Objects;

/**
 * Used to cache LDAP information of user, such as password and privileges.
 */
public class LdapUserInfo {
    public LdapUserInfo(String userName, boolean isSetPasswd, String passwd, PaloRole role) {
        this.userName = userName;
        this.isSetPasswd = isSetPasswd;
        this.passwd = passwd;
        this.role = role;
        this.lastTimeStamp = System.currentTimeMillis();
    }

    private LdapUserInfo(String userName, boolean isSetPasswd, String passwd, PaloRole role, long lastTimeStamp) {
        this.userName = userName;
        this.isSetPasswd = isSetPasswd;
        this.passwd = passwd;
        this.role = role;
        this.lastTimeStamp = lastTimeStamp;
    }

    private final String userName;

    private final boolean isSetPasswd;

    private final String passwd;

    private final PaloRole role;

    private final long lastTimeStamp;

    public String getUserName() {
        return userName;
    }

    // The password needs to be checked by LdapManager for updated cache, so it is visible in the package.
    boolean isSetPasswd() {
        return isSetPasswd;
    }

    String getPasswd() {
        return passwd;
    }

    public PaloRole getPaloRole() {
        return role;
    }

    public LdapUserInfo cloneWithPasswd(String passwd) {
        if (Objects.isNull(passwd)) {
            return new LdapUserInfo(userName, isSetPasswd, this.passwd, role, lastTimeStamp);
        }

        return new LdapUserInfo(userName, true, passwd, role, lastTimeStamp);
    }

    // Return true if LdapUserInfo is exceeded the time limit;
    public boolean checkTimeout() {
        return System.currentTimeMillis() > lastTimeStamp + LdapConfig.ldap_user_cache_timeout_s * 1000;
    }
}

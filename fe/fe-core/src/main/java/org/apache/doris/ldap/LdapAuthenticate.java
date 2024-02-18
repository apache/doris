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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * This class is used for LDAP authentication login and LDAP group authorization.
 * This means that users can log in to Doris with a user name and LDAP password,
 * and the user will get the privileges of all roles corresponding to the LDAP group.
 */
public class LdapAuthenticate {
    private static final Logger LOG = LogManager.getLogger(LdapAuthenticate.class);

    /**
     * The LDAP authentication process is as follows:
     * step1: Check the LDAP password.
     * step2: Get the LDAP groups privileges as a role, saved into ConnectContext.
     * step3: Set current userIdentity. If the user account does not exist in Doris, login as a temporary user.
     * Otherwise, login to the Doris account.
     */
    public static boolean authenticate(ConnectContext context, String password, String qualifiedUser) {
        String usePasswd = (Strings.isNullOrEmpty(password)) ? "NO" : "YES";
        String userName = ClusterNamespace.getNameFromFullName(qualifiedUser);
        LOG.debug("user:{}", userName);

        // check user password by ldap server.
        try {
            if (!Env.getCurrentEnv().getAuth().getLdapManager().checkUserPasswd(qualifiedUser, password)) {
                LOG.info("user:{} use check LDAP password failed.", userName);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, qualifiedUser, context.getRemoteIP(), usePasswd);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Check ldap password error.", e);
            return false;
        }

        String remoteIp = context.getMysqlChannel().getRemoteIp();
        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        // Search the user in doris.
        List<UserIdentity> userIdentities = Env.getCurrentEnv().getAuth()
                .getUserIdentityForLdap(qualifiedUser, remoteIp);
        UserIdentity userIdentity;
        if (userIdentities.isEmpty()) {
            userIdentity = tempUserIdentity;
            LOG.debug("User:{} does not exists in doris, login as temporary users.", userName);
            context.setIsTempUser(true);
        } else {
            userIdentity = userIdentities.get(0);
        }

        context.setCurrentUserIdentity(userIdentity);
        context.setRemoteIP(remoteIp);
        LOG.debug("ldap authentication success: identity:{}", context.getCurrentUserIdentity());
        return true;
    }
}

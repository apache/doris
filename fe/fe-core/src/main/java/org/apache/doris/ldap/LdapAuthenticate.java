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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.PaloRole;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

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

    private static final String LDAP_GROUPS_PRIVS_NAME = "ldapGroupsPrivs";

    // Maximum number of the user LDAP authentication login connections.
    private static long userMaxConn = 100;

    {
        if (LdapConfig.user_max_connections <= 0 || LdapConfig.user_max_connections > 10000) {
            LOG.warn("Ldap config user_max_connections is invalid. It should be set between 1 and 10000. " +
                    "And now, it is set to the default value.");
        } else {
            userMaxConn = LdapConfig.user_max_connections;
        }
    }

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
        String clusterName = ClusterNamespace.getClusterNameFromFullName(qualifiedUser);
        LOG.debug("user:{}, cluster:{}", userName, clusterName);

        // check user password by ldap server.
        try {
            if (!LdapClient.checkPassword(userName, password)) {
                LOG.debug("user:{} use error LDAP password.", userName);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, qualifiedUser, context.getRemoteIP(), usePasswd);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Check ldap password error.", e);
            return false;
        }

        // Get the LDAP groups privileges as a role.
        PaloRole ldapGroupsPrivs;
        try {
            ldapGroupsPrivs = getLdapGroupsPrivs(userName, clusterName);
        } catch (Exception e) {
            LOG.error("Get ldap groups error.", e);
            return false;
        }

        String remoteIp = context.getMysqlChannel().getRemoteIp();
        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        // Search the user in doris.
        UserIdentity userIdentity = Catalog.getCurrentCatalog().getAuth().getCurrentUserIdentity(tempUserIdentity);
        if (userIdentity == null) {
            userIdentity = tempUserIdentity;
            LOG.debug("User:{} does not exists in doris, login as temporary users.", userName);
            context.setIsTempUser(true);
            if (ldapGroupsPrivs == null) {
                ldapGroupsPrivs = new PaloRole(LDAP_GROUPS_PRIVS_NAME);
            }
            LdapPrivsChecker.grantDefaultPrivToTempUser(ldapGroupsPrivs, clusterName);
        }

        context.setCurrentUserIdentity(userIdentity);
        context.setRemoteIP(remoteIp);
        context.setLdapGroupsPrivs(ldapGroupsPrivs);
        LOG.debug("ldap authentication success: identity:{}, privs:{}",
                context.getCurrentUserIdentity(), context.getLdapGroupsPrivs());
        return true;
    }

    /**
     * Step1: get ldap groups from ldap server;
     * Step2: get roles by ldap groups;
     * Step3: merge the roles;
     */
    private static PaloRole getLdapGroupsPrivs(String userName, String clusterName) {
        //get user ldap group. the ldap group name should be the same as the doris role name
        List<String> ldapGroups = LdapClient.getGroups(userName);
        List<String> rolesNames = Lists.newArrayList();
        for (String group : ldapGroups) {
            String qualifiedRole = ClusterNamespace.getFullName(clusterName, group);
            if (Catalog.getCurrentCatalog().getAuth().doesRoleExist(qualifiedRole)) {
                rolesNames.add(qualifiedRole);
            }
        }
        LOG.debug("get user:{} ldap groups:{} and doris roles:{}", userName, ldapGroups, rolesNames);

        // merge the roles
        if (rolesNames.isEmpty()) {
            return null;
        } else {
            PaloRole ldapGroupsPrivs = new PaloRole(LDAP_GROUPS_PRIVS_NAME);
            Catalog.getCurrentCatalog().getAuth().mergeRolesNoCheckName(rolesNames, ldapGroupsPrivs);
            return ldapGroupsPrivs;
        }
    }

    public static long getMaxConn() {
        return userMaxConn;
    }
}

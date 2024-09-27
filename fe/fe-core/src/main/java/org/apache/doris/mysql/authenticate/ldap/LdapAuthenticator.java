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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.Authenticator;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.authenticate.password.ClearPasswordResolver;
import org.apache.doris.mysql.authenticate.password.Password;
import org.apache.doris.mysql.authenticate.password.PasswordResolver;
import org.apache.doris.mysql.privilege.Auth;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * This class is used for LDAP authentication login and LDAP group authorization.
 * This means that users can log in to Doris with a user name and LDAP password,
 * and the user will get the privileges of all roles corresponding to the LDAP group.
 */
public class LdapAuthenticator implements Authenticator {
    private static final Logger LOG = LogManager.getLogger(LdapAuthenticator.class);

    private PasswordResolver passwordResolver;

    public LdapAuthenticator() {
        this.passwordResolver = new ClearPasswordResolver();
    }

    /*
     * ldap:
     * server ---AuthSwitch---> client
     * server <--- clear text password --- client
     */
    @Override
    public AuthenticateResponse authenticate(AuthenticateRequest request) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{} start to ldap authenticate.", request.getUserName());
        }
        Password password = request.getPassword();
        if (!(password instanceof ClearPassword)) {
            return AuthenticateResponse.failedResponse;
        }
        ClearPassword clearPassword = (ClearPassword) password;
        return internalAuthenticate(clearPassword.getPassword(), request.getUserName(), request.getRemoteIp());
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        if (qualifiedUser.equals(Auth.ROOT_USER) || qualifiedUser.equals(Auth.ADMIN_USER)) {
            return false;
        }
        if (!Env.getCurrentEnv().getAuth().getLdapManager().doesUserExist(qualifiedUser)) {
            return false;
        }
        return true;
    }

    /**
     * The LDAP authentication process is as follows:
     * step1: Check the LDAP password.
     * step2: Get the LDAP groups privileges as a role, saved into ConnectContext.
     * step3: Set current userIdentity. If the user account does not exist in Doris, login as a temporary user.
     * Otherwise, login to the Doris account.
     */
    private AuthenticateResponse internalAuthenticate(String password, String qualifiedUser, String remoteIp) {
        String usePasswd = (Strings.isNullOrEmpty(password)) ? "NO" : "YES";
        String userName = ClusterNamespace.getNameFromFullName(qualifiedUser);
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{}", userName);
        }

        // check user password by ldap server.
        try {
            if (!Env.getCurrentEnv().getAuth().getLdapManager().checkUserPasswd(qualifiedUser, password)) {
                LOG.info("user:{} use check LDAP password failed.", userName);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, qualifiedUser, remoteIp, usePasswd);
                return AuthenticateResponse.failedResponse;
            }
        } catch (Exception e) {
            LOG.error("Check ldap password error.", e);
            return AuthenticateResponse.failedResponse;
        }

        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        // Search the user in doris.
        List<UserIdentity> userIdentities = Env.getCurrentEnv().getAuth()
                .getUserIdentityForLdap(qualifiedUser, remoteIp);
        AuthenticateResponse response = new AuthenticateResponse(true);
        if (userIdentities.isEmpty()) {
            response.setUserIdentity(tempUserIdentity);
            if (LOG.isDebugEnabled()) {
                LOG.debug("User:{} does not exists in doris, login as temporary users.", userName);
            }
            response.setTemp(true);
        } else {
            response.setUserIdentity(userIdentities.get(0));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("ldap authentication success: identity:{}", response.getUserIdentity());
        }
        return response;
    }

    @Override
    public PasswordResolver getPasswordResolver() {
        return passwordResolver;
    }
}

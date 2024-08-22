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

package org.apache.doris.common.security.authentication;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

@Deprecated
public class HadoopUGI {
    private static final Logger LOG = LogManager.getLogger(HadoopUGI.class);

    /**
     * login and return hadoop ugi
     * @param config auth config
     * @return ugi
     */
    private static UserGroupInformation loginWithUGI(AuthenticationConfig config) {
        if (config == null || !config.isValid()) {
            return null;
        }
        if (config instanceof KerberosAuthenticationConfig) {
            try {
                // TODO: remove after iceberg and hudi kerberos test case pass
                try {
                    //  login hadoop with keytab and try checking TGT
                    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                    LOG.debug("Current login user: {}", ugi.getUserName());
                    String principal = ((KerberosAuthenticationConfig) config).getKerberosPrincipal();
                    if (ugi.hasKerberosCredentials() && StringUtils.equals(ugi.getUserName(), principal)) {
                        // if the current user is logged by kerberos and is the same user
                        // just use checkTGTAndReloginFromKeytab because this method will only relogin
                        // when the TGT is expired or is close to expiry
                        ugi.checkTGTAndReloginFromKeytab();
                        return ugi;
                    }
                } catch (IOException e) {
                    LOG.warn("A SecurityException occurs with kerberos, do login immediately.", e);
                }
                return new HadoopKerberosAuthenticator((KerberosAuthenticationConfig) config).getUGI();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            String hadoopUserName = ((SimpleAuthenticationConfig) config).getUsername();
            if (hadoopUserName == null) {
                hadoopUserName = "hadoop";
                ((SimpleAuthenticationConfig) config).setUsername(hadoopUserName);
                LOG.debug(AuthenticationConfig.HADOOP_USER_NAME + " is unset, use default user: hadoop");
            }

            UserGroupInformation ugi;
            try {
                ugi = UserGroupInformation.getLoginUser();
                if (ugi.getUserName().equals(hadoopUserName)) {
                    return ugi;
                }
            } catch (IOException e) {
                LOG.warn("A SecurityException occurs with simple, do login immediately.", e);
            }

            ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
            LOG.debug("Login by proxy user, hadoop.username: {}", hadoopUserName);
            return ugi;
        }
    }

    public static <T> T ugiDoAs(AuthenticationConfig authConf, PrivilegedExceptionAction<T> action) {
        UserGroupInformation ugi = HadoopUGI.loginWithUGI(authConf);
        try {
            if (ugi != null) {
                if (authConf instanceof KerberosAuthenticationConfig) {
                    ugi.checkTGTAndReloginFromKeytab();
                }
                return ugi.doAs(action);
            } else {
                return action.run();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

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
        UserGroupInformation ugi;
        if (config instanceof KerberosAuthenticationConfig) {
            KerberosAuthenticationConfig krbConfig = (KerberosAuthenticationConfig) config;
            Configuration hadoopConf = krbConfig.getConf();
            hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
            hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, "true");
            UserGroupInformation.setConfiguration(hadoopConf);
            String principal = krbConfig.getKerberosPrincipal();
            try {
                //  login hadoop with keytab and try checking TGT
                ugi = UserGroupInformation.getLoginUser();
                LOG.debug("Current login user: {}", ugi.getUserName());
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
            try {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, krbConfig.getKerberosKeytab());
                UserGroupInformation.setLoginUser(ugi);
                LOG.debug("Login by kerberos authentication with principal: {}", principal);
                return ugi;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            String hadoopUserName = ((SimpleAuthenticationConfig) config).getUsername();
            if (hadoopUserName == null) {
                hadoopUserName = "hadoop";
                LOG.debug(AuthenticationConfig.HADOOP_USER_NAME + " is unset, use default user: hadoop");
            }
            ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
            LOG.debug("Login by proxy user, hadoop.username: {}", hadoopUserName);
            return ugi;
        }
    }

    /**
     * use for HMSExternalCatalog to login
     * @param config auth config
     */
    public static void tryKrbLogin(String catalogName, AuthenticationConfig config) {
        if (config instanceof KerberosAuthenticationConfig) {
            KerberosAuthenticationConfig krbConfig = (KerberosAuthenticationConfig) config;
            try {
                Configuration hadoopConf = krbConfig.getConf();
                hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
                hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, "true");
                UserGroupInformation.setConfiguration(hadoopConf);
                /**
                 * Because metastore client is created by using
                 * {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient#getProxy}
                 * it will relogin when TGT is expired, so we don't need to relogin manually.
                 */
                UserGroupInformation.loginUserFromKeytab(krbConfig.getKerberosPrincipal(),
                        krbConfig.getKerberosKeytab());
            } catch (IOException e) {
                throw new RuntimeException("login with kerberos auth failed for catalog: " + catalogName, e);
            }
        }
    }

    public static <T> T ugiDoAs(AuthenticationConfig authConf, PrivilegedExceptionAction<T> action) {
        UserGroupInformation ugi = HadoopUGI.loginWithUGI(authConf);
        try {
            if (ugi != null) {
                ugi.checkTGTAndReloginFromKeytab();
                return ugi.doAs(action);
            } else {
                return action.run();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}

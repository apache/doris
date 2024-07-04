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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class HadoopKerberosAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopKerberosAuthenticator.class);
    private final KerberosAuthenticationConfig config;
    private final UserGroupInformation ugi;

    public HadoopKerberosAuthenticator(KerberosAuthenticationConfig config) {
        this.config = config;
        try {
            ugi = getUGI();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void initializeAuthConfig(Configuration hadoopConf) {
        hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
        hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_KERBEROS_KEYTAB_LOGIN_AUTORENEWAL_ENABLED, "true");
        synchronized (HadoopKerberosAuthenticator.class) {
            // avoid other catalog set conf at the same time
            UserGroupInformation.setConfiguration(hadoopConf);
        }
    }

    @Override
    public UserGroupInformation getUGI() throws IOException {
        // login and get ugi when catalog is initialized
        initializeAuthConfig(config.getConf());
        String principal = config.getKerberosPrincipal();
        Subject subject = getSubject(config.getKerberosKeytab(), principal);
        LOG.debug("Login by kerberos authentication with principal: {}", principal);
        return UserGroupInformation.getUGIFromSubject(subject);
    }

    protected static Subject getSubject(String keytab, String principal) {
        Subject subject = new Subject(false, ImmutableSet.of(new KerberosPrincipal(principal)),
                Collections.emptySet(), Collections.emptySet());
        javax.security.auth.login.Configuration conf = getConfiguration(keytab, principal);
        try {
            LoginContext loginContext = new LoginContext("", subject, null, conf);
            loginContext.login();
            return loginContext.getSubject();
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    protected static javax.security.auth.login.Configuration getConfiguration(String keytab, String principal) {
        return new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, String> options = ImmutableMap.<String, String>builder()
                        .put("doNotPrompt", "true")
                        .put("isInitiator", "true")
                        .put("useKeyTab", "true")
                        .put("storeKey", "true")
                        .put("keyTab", keytab)
                        .put("principal", principal)
                        // .put("debug", "true")
                        .build();
                return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)};
            }
        };
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
        return ugi.doAs(action);
    }
}

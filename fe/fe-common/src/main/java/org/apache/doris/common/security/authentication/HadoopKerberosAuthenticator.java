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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.authentication.KerberosTicketUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class HadoopKerberosAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopKerberosAuthenticator.class);
    private final KerberosAuthenticationConfig config;
    private Subject subject;
    private long nextRefreshTime;
    private UserGroupInformation ugi;

    public HadoopKerberosAuthenticator(KerberosAuthenticationConfig config) {
        this.config = config;
    }

    public static void initializeAuthConfig(Configuration hadoopConf) {
        hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
        synchronized (HadoopKerberosAuthenticator.class) {
            // avoid other catalog set conf at the same time
            UserGroupInformation.setConfiguration(hadoopConf);
        }
    }

    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (ugi == null) {
            subject = getSubject(config.getKerberosKeytab(), config.getKerberosPrincipal(), config.isPrintDebugLog());
            ugi = Objects.requireNonNull(login(subject), "login result is null");
            return ugi;
        }
        if (nextRefreshTime < System.currentTimeMillis()) {
            long lastRefreshTime = nextRefreshTime;
            Subject existingSubject = subject;
            if (LOG.isDebugEnabled()) {
                Date lastTicketEndTime = getTicketEndTime(subject);
                LOG.debug("Current ticket expired time is {}", lastTicketEndTime);
            }
            // renew subject
            Subject newSubject = getSubject(config.getKerberosKeytab(), config.getKerberosPrincipal(),
                    config.isPrintDebugLog());
            Objects.requireNonNull(login(newSubject), "re-login result is null");
            // modify UGI instead of returning new UGI
            existingSubject.getPrincipals().addAll(newSubject.getPrincipals());
            Set<Object> privateCredentials = existingSubject.getPrivateCredentials();
            // clear the old credentials
            synchronized (privateCredentials) {
                privateCredentials.clear();
                privateCredentials.addAll(newSubject.getPrivateCredentials());
            }
            Set<Object> publicCredentials = existingSubject.getPublicCredentials();
            synchronized (publicCredentials) {
                publicCredentials.clear();
                publicCredentials.addAll(newSubject.getPublicCredentials());
            }
            nextRefreshTime = calculateNextRefreshTime(newSubject);
            if (LOG.isDebugEnabled()) {
                Date lastTicketEndTime = getTicketEndTime(newSubject);
                LOG.debug("Next ticket expired time is {}", lastTicketEndTime);
                LOG.debug("Refresh kerberos ticket succeeded, last time is {}, next time is {}",
                        lastRefreshTime, nextRefreshTime);
            }
        }
        return ugi;
    }

    private UserGroupInformation login(Subject subject) throws IOException {
        // login and get ugi when catalog is initialized
        initializeAuthConfig(config.getConf());
        String principal = config.getKerberosPrincipal();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Login by kerberos authentication with principal: {}", principal);
        }
        return UserGroupInformation.getUGIFromSubject(subject);
    }

    private static long calculateNextRefreshTime(Subject subject) {
        Preconditions.checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = KerberosTicketUtils.getTicketGrantingTicket(subject);
        return KerberosTicketUtils.getRefreshTime(tgtTicket);
    }

    private static Date getTicketEndTime(Subject subject) {
        Preconditions.checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = KerberosTicketUtils.getTicketGrantingTicket(subject);
        return tgtTicket.getEndTime();
    }

    private static Subject getSubject(String keytab, String principal, boolean printDebugLog) {
        Subject subject = new Subject(false, ImmutableSet.of(new KerberosPrincipal(principal)),
                Collections.emptySet(), Collections.emptySet());
        javax.security.auth.login.Configuration conf = getConfiguration(keytab, principal, printDebugLog);
        try {
            LoginContext loginContext = new LoginContext("", subject, null, conf);
            loginContext.login();
            return loginContext.getSubject();
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    private static javax.security.auth.login.Configuration getConfiguration(String keytab, String principal,
                                                                            boolean printDebugLog) {
        return new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                        .put("doNotPrompt", "true")
                        .put("isInitiator", "true")
                        .put("useKeyTab", "true")
                        .put("storeKey", "true")
                        .put("keyTab", keytab)
                        .put("principal", principal);
                if (printDebugLog) {
                    builder.put("debug", "true");
                }
                Map<String, String> options = builder.build();
                return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)};
            }
        };
    }
}

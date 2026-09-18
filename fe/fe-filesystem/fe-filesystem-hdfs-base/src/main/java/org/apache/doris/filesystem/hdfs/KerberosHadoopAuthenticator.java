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
// This file is based on code available under the Apache license here:
// https://github.com/trinodb/trino/blob/435/lib/trino-plugin-toolkit/src/main/java/io/trino/plugin/base/authentication/KerberosAuthentication.java
// https://github.com/trinodb/trino/blob/435/lib/trino-hdfs/src/main/java/io/trino/hdfs/authentication/CachingKerberosHadoopAuthentication.java
// and modified by Doris

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.foundation.security.ExecutionAuthenticator;
import org.apache.doris.foundation.security.KerberosTicketUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Kerberos-based implementation of {@link ExecutionAuthenticator}.
 *
 * <p>Logs in from a keytab via an explicit JAAS {@code Krb5LoginModule} configuration
 * ({@code doNotPrompt=true}) and proactively refreshes the TGT once it passes 80% of
 * its lifetime, swapping the new credentials into the existing Subject in place. This
 * is a port of trino's {@code KerberosAuthentication} +
 * {@code CachingKerberosHadoopAuthentication}. It deliberately does NOT use
 * {@code UserGroupInformation.checkTGTAndReloginFromKeytab()}: its hard-coded
 * 60-second relogin throttle can leave an expired TGT in the Subject, after which the
 * SASL/GSS layer falls back to the JVM-default interactive JAAS login and fails with
 * "LoginException: Cannot read from System.in".
 *
 * <p>Note: {@link UserGroupInformation#setConfiguration(Configuration)} mutates
 * JVM-global state — all UGI instances in the process share the same authentication
 * mode. Multiple HDFS catalogs with differing {@code hadoop.security.authentication}
 * values therefore cannot truly coexist: the first writer wins. We serialise the
 * setup via {@link #UGI_INIT_LOCK} and skip the {@code setConfiguration} call when
 * the existing auth method already matches, so concurrent Kerberos catalogs don't
 * stomp on each other. When modes disagree a WARN is logged so operators notice.
 */
public class KerberosHadoopAuthenticator implements ExecutionAuthenticator {

    private static final Logger LOG = LogManager.getLogger(KerberosHadoopAuthenticator.class);

    /** Process-wide lock for serialising UGI static setup. */
    private static final Object UGI_INIT_LOCK = new Object();

    private final String principal;
    private final String keytab;

    // The Subject/UGI pair is created once and never replaced: refreshes swap new
    // Kerberos credentials into this same Subject so Hadoop code that caches the
    // UGI (e.g. DFSClient) transparently sees the new ticket.
    private final Subject subject;
    private final UserGroupInformation ugi;
    // Guarded by "this" (only touched in the constructor and synchronized getUGI()).
    private long nextRefreshTime;

    public KerberosHadoopAuthenticator(String principal, String keytab, Configuration conf) {
        this.principal = principal;
        this.keytab = keytab;
        try {
            synchronized (UGI_INIT_LOCK) {
                AuthenticationMethod desired = SecurityUtil.getAuthenticationMethod(conf);
                if (!shouldSkipSetConfiguration(desired)) {
                    UserGroupInformation.setConfiguration(conf);
                }
                this.subject = loginSubject();
                this.ugi = Objects.requireNonNull(UserGroupInformation.getUGIFromSubject(subject),
                        "getUGIFromSubject returned null");
            }
            this.nextRefreshTime = KerberosTicketUtils.getRefreshTime(
                    KerberosTicketUtils.getTicketGrantingTicket(subject));
            LOG.info("Kerberos login succeeded for principal={}", principal);
        } catch (IOException | RuntimeException e) {
            throw new RuntimeException("Failed to login with Kerberos principal=" + principal
                    + ", keytab=" + keytab, e);
        }
    }

    /**
     * Returns true when the JVM-global UGI configuration already matches what this
     * authenticator needs, so we can safely skip the {@code setConfiguration} call.
     * If security is on but the existing auth mode differs, a WARN is logged and
     * we still skip to preserve first-writer-wins semantics — the operator is then
     * responsible for aligning catalog configs.
     */
    private boolean shouldSkipSetConfiguration(AuthenticationMethod desired) {
        if (!UserGroupInformation.isSecurityEnabled()) {
            return false;
        }
        AuthenticationMethod current;
        try {
            current = UserGroupInformation.getLoginUser().getAuthenticationMethod();
        } catch (IOException e) {
            return false;
        }
        if (current == desired) {
            return true;
        }
        LOG.warn("UGI already configured with authentication={} but this catalog requests {}; "
                + "keeping existing JVM-global setting (first-writer-wins).", current, desired);
        return true;
    }

    @Override
    public <T> T execute(Callable<T> task) throws Exception {
        UserGroupInformation currentUgi;
        try {
            currentUgi = getUGI();
        } catch (IOException | RuntimeException e) {
            // Keep the checked-IOException contract of doAs(): a relogin failure (unchecked
            // RuntimeException from the JAAS login) must not escape unchecked.
            throw new IOException("Kerberos relogin failed for principal=" + principal, e);
        }
        try {
            return currentUgi.doAs((PrivilegedExceptionAction<T>) task::call);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Kerberos doAs interrupted for principal=" + principal, e);
        }
    }

    /**
     * Returns the cached UGI, first refreshing the TGT if it is past 80% of its
     * lifetime. Ported from trino's
     * {@code CachingKerberosHadoopAuthentication.getUserGroupInformation()} — note
     * there is intentionally no relogin throttle.
     * If the refresh login fails, {@code nextRefreshTime} is not advanced, so each
     * subsequent call retries the login until the KDC recovers (no backoff) — same
     * trade-off as trino.
     */
    private synchronized UserGroupInformation getUGI() throws IOException {
        if (nextRefreshTime < System.currentTimeMillis()) {
            Subject newSubject = loginSubject();
            Objects.requireNonNull(UserGroupInformation.getUGIFromSubject(newSubject),
                    "getUGIFromSubject returned null");
            // We modify the existing UGI's credentials in-place instead of returning a new UGI
            // because some parts of Hadoop code reuse UGI (e.g. DFSClient).
            // We also need to clear the old credentials because the JDK assumes that the first
            // credential is the TGT, which is not always true.
            subject.getPrincipals().addAll(newSubject.getPrincipals());
            Set<Object> privateCredentials = subject.getPrivateCredentials();
            synchronized (privateCredentials) {
                privateCredentials.clear();
                privateCredentials.addAll(newSubject.getPrivateCredentials());
            }
            Set<Object> publicCredentials = subject.getPublicCredentials();
            synchronized (publicCredentials) {
                publicCredentials.clear();
                publicCredentials.addAll(newSubject.getPublicCredentials());
            }
            nextRefreshTime = KerberosTicketUtils.getRefreshTime(
                    KerberosTicketUtils.getTicketGrantingTicket(newSubject));
            LOG.info("Kerberos ticket refreshed for principal={}, next refresh time={}",
                    principal, nextRefreshTime);
        }
        return ugi;
    }

    /**
     * Performs the JAAS keytab login and returns the logged-in Subject. This is
     * trino's {@code KerberosAuthentication.getSubject()} delegate boundary, kept
     * as a package-private method so tests can substitute fabricated Subjects.
     */
    Subject loginSubject() {
        return getSubject(keytab, principal);
    }

    private static Subject getSubject(String keytab, String principal) {
        Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(principal)),
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

    private static javax.security.auth.login.Configuration getConfiguration(String keytab, String principal) {
        Map<String, String> optionsBuilder = new LinkedHashMap<>();
        optionsBuilder.put("doNotPrompt", "true");
        optionsBuilder.put("isInitiator", "true");
        optionsBuilder.put("principal", principal);
        optionsBuilder.put("useKeyTab", "true");
        optionsBuilder.put("storeKey", "true");
        optionsBuilder.put("keyTab", keytab);
        Map<String, String> options = Collections.unmodifiableMap(optionsBuilder);
        return new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                "com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)};
            }
        };
    }
}

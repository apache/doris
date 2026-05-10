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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.spi.HadoopAuthenticator;
import org.apache.doris.filesystem.spi.IOCallable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Kerberos-based implementation of {@link HadoopAuthenticator}.
 * Logs in from a keytab and executes actions as the Kerberos principal via UGI.doAs().
 *
 * <p>Note: {@link UserGroupInformation#setConfiguration(Configuration)} mutates
 * JVM-global state — all UGI instances in the process share the same authentication
 * mode. Multiple HDFS catalogs with differing {@code hadoop.security.authentication}
 * values therefore cannot truly coexist: the first writer wins. We serialise the
 * setup via {@link #UGI_INIT_LOCK} and skip the {@code setConfiguration} call when
 * the existing auth method already matches, so concurrent Kerberos catalogs don't
 * stomp on each other. When modes disagree a WARN is logged so operators notice.
 */
public class KerberosHadoopAuthenticator implements HadoopAuthenticator {

    private static final Logger LOG = LogManager.getLogger(KerberosHadoopAuthenticator.class);

    /** Process-wide lock for serialising UGI static setup. */
    private static final Object UGI_INIT_LOCK = new Object();

    private final String principal;
    private final String keytab;
    private volatile UserGroupInformation ugi;

    public KerberosHadoopAuthenticator(String principal, String keytab, Configuration conf) {
        this.principal = principal;
        this.keytab = keytab;
        try {
            synchronized (UGI_INIT_LOCK) {
                AuthenticationMethod desired = SecurityUtil.getAuthenticationMethod(conf);
                if (!shouldSkipSetConfiguration(desired)) {
                    UserGroupInformation.setConfiguration(conf);
                }
                this.ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
            }
            LOG.info("Kerberos login succeeded for principal={}", principal);
        } catch (IOException e) {
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
    public <T> T doAs(IOCallable<T> action) throws IOException {
        // Refresh the Kerberos TGT from the keytab if it's close to expiry. This is
        // a no-op when the ticket is still valid, so it's safe and cheap to call on
        // every request and avoids long-lived FE processes failing with
        // "GSSException: No valid credentials" after the initial ticket expires.
        try {
            ugi.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
            throw new IOException("Kerberos relogin failed for principal=" + principal, e);
        }
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) action::call);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Kerberos doAs interrupted for principal=" + principal, e);
        }
    }
}

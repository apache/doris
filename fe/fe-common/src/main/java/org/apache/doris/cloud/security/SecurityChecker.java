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

package org.apache.doris.cloud.security;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * SecurityChecker is used to check if the url is safe
 */
public abstract class SecurityChecker {
    private static final Logger LOG = LogManager.getLogger(SecurityChecker.class);

    // Upper bound for one SSRF connectivity probe. The probe only needs the net-hook to observe the
    // resolved IP before the TCP handshake completes; a short timeout keeps CREATE/ALTER responsive when
    // the endpoint is a public-but-unreachable host (that outcome is ignored, see checkEndpointForSsrf).
    private static final int SSRF_PROBE_CONNECT_TIMEOUT_MS = 5000;

    private static volatile SecurityChecker instance;

    protected SecurityChecker() {}

    public static SecurityChecker getInstance() {
        if (instance == null) {
            synchronized (SecurityChecker.class) {
                if (instance == null) {
                    instance = Strings.isNullOrEmpty(Config.security_checker_class_name)
                            ? new DummySecurityChecker() : new UrlSecurityChecker();
                }
            }
        }
        return instance;
    }

    @VisibleForTesting
    public static void setInstanceForTest(SecurityChecker checker) {
        instance = checker;
    }

    /**
     * Check and return safe jdbc url, avoid sql injection or other security issues
     * @param originJdbcUrl
     * @return
     * @throws Exception
     */
    public abstract String getSafeJdbcUrl(String originJdbcUrl) throws Exception;

    /**
     * Install the net-hook for an http(s) request on the current thread, avoid SSRF attack.
     * Only handle http:// https://. Must bracket a real request: startSSRFChecking before issuing it,
     * {@link #stopSSRFChecking()} after it, and no further request may be issued once stopped.
     * @param originUri
     * @throws Exception
     */
    public abstract void startSSRFChecking(String originUri) throws Exception;

    /**
     * Install the net-hook for a raw TCP connection on the current thread, avoid SSRF attack.
     * Handles any protocol that connects through a JDK socket (thrift, jdbc, ...), not only http.
     * Same bracketing contract as {@link #startSSRFChecking(String)}.
     * @throws Exception
     */
    public abstract void startSSRFChecking() throws Exception;

    public abstract void stopSSRFChecking();

    /**
     * Whether a real SSRF checker (the third-party SDK) is installed. When false every
     * {@link #checkEndpointForSsrf(String, int)} is a no-op, so the default open-source build behaves
     * exactly as before.
     */
    public abstract boolean isEnabled();

    /**
     * Point-in-time SSRF gate for a single user-supplied endpoint: brackets a lightweight TCP connect
     * with the no-arg net-hook so the SDK resolves the host and rejects an intranet target before the
     * handshake completes.
     *
     * <p>The SDK signals an intranet target by throwing {@code SSRFUnsafeConnectionException} (a
     * {@link SecurityException}) from inside the connect; that — and only that — is the rejection. A plain
     * connectivity failure (refused / timeout / unknown host) is NOT an SSRF signal and is ignored, so a
     * catalog that points at a currently-unreachable but public endpoint can still be created.
     */
    public final void checkEndpointForSsrf(String host, int port) throws DdlException {
        if (!isEnabled()) {
            return;
        }
        try {
            startSSRFChecking();
        } catch (Exception e) {
            // isEnabled() is true but the SDK net-hook could not be installed: surface the misconfiguration
            // rather than silently skipping the security gate.
            throw new DdlException("Failed to initialize SSRF checking for endpoint " + host + ":" + port
                    + ": " + e.getMessage(), e);
        }
        try {
            doConnect(host, port, SSRF_PROBE_CONNECT_TIMEOUT_MS);
        } catch (SecurityException e) {
            throw new DdlException("Endpoint " + host + ":" + port
                    + " is rejected by the SSRF policy (intranet address not allowed): " + e.getMessage());
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SSRF probe to {}:{} did not complete, ignored (not an SSRF rejection): {}",
                        host, port, e.toString());
            }
        } finally {
            stopSSRFChecking();
        }
    }

    /**
     * The connection seam of {@link #checkEndpointForSsrf(String, int)}: a single real TCP connect the
     * net-hook can observe. Overridable so unit tests can drive the SSRF / unreachable / success branches
     * without the third-party SDK or a live server.
     */
    protected void doConnect(String host, int port, int connectTimeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        }
    }
}

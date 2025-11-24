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

/*
 * Portions Copyright 2021 The gRPC Authors.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * See above for details.
 *
 * NOTE:
 *   This file is largely based on gRPC source code:
 *   https://github.com/grpc/grpc-java/blob/master/util/src/main/java/io/grpc/util/AdvancedTlsX509TrustManager.java
 *   It has been adapted to fit this project's requirements.
 *
 * Modifications copyright (c) 2025 SelectDB.
 */

package org.apache.doris.common.util;

import io.grpc.util.CertificateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * X509TlsReloadableTrustManager is an {@code X509ExtendedTrustManager} that allows users to configure
 * advanced TLS features, such as root certificate reloading and peer cert custom verification.
 * The basic instantiation pattern is
 * <code>new Builder().build().useSystemDefaultTrustCerts();</code>
 *
 * <p>For Android users: this class is only supported in API level 24 and above.
 */
public final class X509TlsReloadableTrustManager extends X509ExtendedTrustManager {
    private static final Logger LOG = LogManager.getLogger(X509TlsReloadableTrustManager.class.getName());

    // Minimum allowed period for refreshing files with credential information.
    private static final int MINIMUM_REFRESH_PERIOD_IN_MINUTES = 1;
    private static final String NOT_ENOUGH_INFO_MESSAGE =
            "Not enough information to validate peer. SSLEngine or Socket required.";
    private final Verification verification;
    private final SslSocketAndEnginePeerVerifier socketAndEnginePeerVerifier;

    // The delegated trust manager used to perform traditional certificate verification.
    private volatile X509ExtendedTrustManager delegateManager = null;

    private X509TlsReloadableTrustManager(Verification verification,
            SslSocketAndEnginePeerVerifier socketAndEnginePeerVerifier) {
        this.verification = verification;
        this.socketAndEnginePeerVerifier = socketAndEnginePeerVerifier;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        throw new CertificateException(NOT_ENOUGH_INFO_MESSAGE);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        checkTrusted(chain, authType, null, socket, false);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        checkTrusted(chain, authType, engine, null, false);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain,  String authType, SSLEngine engine)
            throws CertificateException {
        checkTrusted(chain, authType, engine, null, true);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {
        throw new CertificateException(NOT_ENOUGH_INFO_MESSAGE);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        checkTrusted(chain, authType, null, socket, true);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        if (this.delegateManager == null) {
            return new X509Certificate[0];
        }
        return this.delegateManager.getAcceptedIssuers();
    }

    /**
     * Uses the default trust certificates stored on user's local system.
     * After this is used, functions that will provide new credential
     * data(e.g. updateTrustCredentials) should not be called.
     */
    public void useSystemDefaultTrustCerts() throws CertificateException, KeyStoreException,
            NoSuchAlgorithmException {
        // Passing a null value of KeyStore would make {@code TrustManagerFactory} attempt to use
        // system-default trust CA certs.
        this.delegateManager = createDelegateTrustManager(null);
    }

    private static X509ExtendedTrustManager createDelegateTrustManager(KeyStore keyStore)
            throws CertificateException, KeyStoreException, NoSuchAlgorithmException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        X509ExtendedTrustManager delegateManager = null;
        TrustManager[] tms = tmf.getTrustManagers();
        // Iterate over the returned trust managers, looking for an instance of X509TrustManager.
        // If found, use that as the delegate trust manager.
        for (TrustManager tm : tms) {
            if (tm instanceof X509ExtendedTrustManager) {
                delegateManager = (X509ExtendedTrustManager) tm;
                break;
            }
        }
        if (delegateManager == null) {
            throw new CertificateException(
                "Failed to find X509ExtendedTrustManager with default TrustManager algorithm "
                    + TrustManagerFactory.getDefaultAlgorithm());
        }
        return delegateManager;
    }

    private void checkTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine,
            Socket socket, boolean checkingServer) throws CertificateException {
        if (chain == null || chain.length == 0) {
            throw new IllegalArgumentException(
                "Want certificate verification but got null or empty certificates");
        }
        if (sslEngine == null && socket == null) {
            throw new CertificateException(NOT_ENOUGH_INFO_MESSAGE);
        }
        if (this.verification != Verification.INSECURELY_SKIP_ALL_VERIFICATION) {
            X509ExtendedTrustManager currentDelegateManager = this.delegateManager;
            if (currentDelegateManager == null) {
                throw new CertificateException("No trust roots configured");
            }
            if (checkingServer) {
                String algorithm = this.verification == Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION
                        ? "HTTPS" : "";
                if (sslEngine != null) {
                    SSLParameters sslParams = sslEngine.getSSLParameters();
                    sslParams.setEndpointIdentificationAlgorithm(algorithm);
                    sslEngine.setSSLParameters(sslParams);
                    currentDelegateManager.checkServerTrusted(chain, authType, sslEngine);
                } else {
                    if (!(socket instanceof SSLSocket)) {
                        throw new CertificateException("socket is not a type of SSLSocket");
                    }
                    SSLSocket sslSocket = (SSLSocket) socket;
                    SSLParameters sslParams = sslSocket.getSSLParameters();
                    sslParams.setEndpointIdentificationAlgorithm(algorithm);
                    sslSocket.setSSLParameters(sslParams);
                    currentDelegateManager.checkServerTrusted(chain, authType, sslSocket);
                }
            } else {
                if (sslEngine != null) {
                    currentDelegateManager.checkClientTrusted(chain, authType, sslEngine);
                } else {
                    currentDelegateManager.checkClientTrusted(chain, authType, socket);
                }
            }
        }
        // Perform the additional peer cert check.
        if (socketAndEnginePeerVerifier != null) {
            if (sslEngine != null) {
                socketAndEnginePeerVerifier.verifyPeerCertificate(chain, authType, sslEngine);
            } else {
                socketAndEnginePeerVerifier.verifyPeerCertificate(chain, authType, socket);
            }
        }
    }

    static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Updates the current cached trust certificates as well as the key store.
     *
     * @param trustCerts the trust certificates that are going to be used
     */
    public void updateTrustCredentials(X509Certificate[] trustCerts) throws IOException,
            GeneralSecurityException {
        checkNotNull(trustCerts, "trustCerts");
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        int i = 1;
        for (X509Certificate cert : trustCerts) {
            String alias = Integer.toString(i);
            keyStore.setCertificateEntry(alias, cert);
            i++;
        }
        this.delegateManager = createDelegateTrustManager(keyStore);
        LOG.info("Update Trust Manager");
    }

    /**
     * Updates the trust certificates from a local file path.
     *
     * @param trustCertFile  the file on disk holding the trust certificates
     */
    public void updateTrustCredentials(File trustCertFile) throws IOException,
            GeneralSecurityException {
        long updatedTime = readAndUpdate(trustCertFile, 0);
        if (updatedTime == 0) {
            throw new GeneralSecurityException(
                "Files were unmodified before their initial update. Probably a bug.");
        }
    }

    /**
     * Schedules a {@code ScheduledExecutorService} to read trust certificates from a local file path
     * periodically, and updates the cached trust certs if there is an update. You must close the
     * returned Closeable before calling this method again or other update methods
     * ({@link X509TlsReloadableTrustManager#useSystemDefaultTrustCerts()},
     * {@link X509TlsReloadableTrustManager#updateTrustCredentials(X509Certificate[])},
     * Before scheduling the task, the method synchronously reads and updates trust certificates once.
     * If the provided period is less than 1 minute, it is automatically adjusted to 1 minute.
     *
     * @param trustCertFile  the file on disk holding the trust certificates
     * @param period the period between successive read-and-update executions
     * @param unit the time unit of the initialDelay and period parameters
     * @param executor the executor service we use to read and update the credentials
     * @return an object that caller should close when the file refreshes are not needed
     */
    public Closeable updateTrustCredentials(File trustCertFile, long period, TimeUnit unit,
            ScheduledExecutorService executor) throws IOException, GeneralSecurityException {
        long updatedTime = readAndUpdate(trustCertFile, 0);
        if (updatedTime == 0) {
            executor.shutdown();
            throw new GeneralSecurityException(
                "Files were unmodified before their initial update. Probably a bug.");
        }
        if (period > 0) {
            final ScheduledFuture<?> future =
                    checkNotNull(executor, "executor").scheduleWithFixedDelay(
                        new LoadFilePathExecution(trustCertFile, updatedTime), period, period, unit);
            return () -> {
                future.cancel(false);
                executor.shutdown();
            };
        }
        LOG.warn("SSL certificate reload thread cancle because certificate_reload_interval_s<=0");
        executor.shutdown();
        return () -> {};
    }

    private class LoadFilePathExecution implements Runnable {
        File file;
        long currentTime;

        public LoadFilePathExecution(File file, long currentTime) {
            this.file = file;
            this.currentTime = currentTime;
        }

        @Override
        public void run() {
            try {
                this.currentTime = readAndUpdate(this.file, this.currentTime);
            } catch (IOException | GeneralSecurityException e) {
                LOG.warn(String.format("Failed refreshing trust CAs from file. Using "
                        + "previous CAs (file lastModified = %s)", file.lastModified()), e);
            }
        }
    }

    /**
     * Reads the trust certificates specified in the path location, and updates the key store if the
     * modified time has changed since last read.
     *
     * @param trustCertFile  the file on disk holding the trust certificates
     * @param oldTime the time when the trust file is modified during last execution
     * @return oldTime if failed or the modified time is not changed, otherwise the new modified time
     */
    private long readAndUpdate(File trustCertFile, long oldTime)
            throws IOException, GeneralSecurityException {
        long newTime = checkNotNull(trustCertFile, "trustCertFile").lastModified();
        if (newTime == 0) {
            throw new IOException(
                "Certificate file not found or not readable: " + trustCertFile.getAbsolutePath());
        }
        if (newTime == oldTime) {
            return oldTime;
        }
        FileInputStream inputStream = new FileInputStream(trustCertFile);
        try {
            X509Certificate[] certificates = CertificateUtils.getX509Certificates(inputStream);
            updateTrustCredentials(certificates);
            return newTime;
        } finally {
            inputStream.close();
        }
    }

    // Mainly used to avoid throwing IO Exceptions in java.io.Closeable.
    public interface Closeable extends java.io.Closeable {
        @Override
        void close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * The verification mode when authenticating the peer certificate.
     */
    public enum Verification {
        /**
         * This is the DEFAULT and RECOMMENDED mode for most applications.
         * Setting this on the client side performs both certificate and hostname verification, while
         * setting it on the server side only performs certificate verification.
         */
        CERTIFICATE_AND_HOST_NAME_VERIFICATION,
        /**
         * DANGEROUS: Use trusted credentials to verify the certificate, but clients will not verify the
         * certificate is for the expected host. This setting is only appropriate when accompanied by
         * proper additional peer identity checks set through SslSocketAndEnginePeerVerifier. Failing to
         * do so will leave your applications vulnerable to MITM attacks.
         * This setting has the same behavior on server-side as CERTIFICATE_AND_HOST_NAME_VERIFICATION.
         */
        CERTIFICATE_ONLY_VERIFICATION,
        /**
         * DANGEROUS: This SHOULD be used by advanced user intended to implement the entire verification
         * logic themselves {@link SslSocketAndEnginePeerVerifier}) themselves. This includes: <br>
         * 1. Proper verification of the peer certificate chain <br>
         * 2. Proper checks of the identity of the peer certificate <br>
         * Failing to do so will leave your application without any TLS-related protection. Keep in mind
         * that any loaded trust certificates will be ignored when using this mode.
         */
        INSECURELY_SKIP_ALL_VERIFICATION,
    }

    // Additional custom peer verification check.
    // It will be used when checkClientTrusted/checkServerTrusted is called with the {@code Socket} or
    // the {@code SSLEngine} parameter.
    public interface SslSocketAndEnginePeerVerifier {
        /**
         * Verifies the peer certificate chain. For more information, please refer to
         * {@code X509ExtendedTrustManager}.
         *
         * @param  peerCertChain  the certificate chain sent from the peer
         * @param  authType the key exchange algorithm used, e.g. "RSA", "DHE_DSS", etc
         * @param  socket the socket used for this connection. This parameter can be null, which
         *         indicates that implementations need not check the ssl parameters
         */
        void verifyPeerCertificate(X509Certificate[] peerCertChain,  String authType, Socket socket)
                throws CertificateException;

        /**
         * Verifies the peer certificate chain. For more information, please refer to
         * {@code X509ExtendedTrustManager}.
         *
         * @param  peerCertChain  the certificate chain sent from the peer
         * @param  authType the key exchange algorithm used, e.g. "RSA", "DHE_DSS", etc
         * @param  engine the engine used for this connection. This parameter can be null, which
         *         indicates that implementations need not check the ssl parameters
         */
        void verifyPeerCertificate(X509Certificate[] peerCertChain, String authType, SSLEngine engine)
                throws CertificateException;
    }

    /**
     * Builds a new {@link X509TlsReloadableTrustManager}. By default, no trust certificates are loaded
     * after the build. To load them, use one of the following methods: {@link
     * X509TlsReloadableTrustManager#updateTrustCredentials(X509Certificate[])}, {@link
     * X509TlsReloadableTrustManager#updateTrustCredentials(File, long, TimeUnit,
     * ScheduledExecutorService)}, {@link X509TlsReloadableTrustManager#updateTrustCredentials
     * (File, long, TimeUnit, ScheduledExecutorService)}.
     */
    public static final class Builder {

        private Verification verification = Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION;
        private SslSocketAndEnginePeerVerifier socketAndEnginePeerVerifier;

        private Builder() {}

        /**
         * Sets {@link Verification}, mode when authenticating the peer certificate. By default, {@link
         * Verification#CERTIFICATE_AND_HOST_NAME_VERIFICATION} value is used.
         *
         * @param  verification Verification mode used for the current X509TlsReloadableTrustManager
         * @return Builder with set verification
         */
        public Builder setVerification(Verification verification) {
            this.verification = verification;
            return this;
        }

        /**
         * Sets {@link SslSocketAndEnginePeerVerifier}, which methods will be called in addition to
         * verifying certificates.
         *
         * @param  verifier SslSocketAndEnginePeerVerifier used for the current
         *         X509TlsReloadableTrustManager
         * @return Builder with set verifier
         */
        public Builder setSslSocketAndEnginePeerVerifier(SslSocketAndEnginePeerVerifier verifier) {
            this.socketAndEnginePeerVerifier = verifier;
            return this;
        }

        public X509TlsReloadableTrustManager build() throws CertificateException {
            return new X509TlsReloadableTrustManager(this.verification, this.socketAndEnginePeerVerifier);
        }
    }
}

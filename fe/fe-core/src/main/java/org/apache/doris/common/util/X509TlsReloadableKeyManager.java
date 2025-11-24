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
 *   https://github.com/grpc/grpc-java/blob/master/util/src/main/java/io/grpc/util/AdvancedTlsX509KeyManager.java.
 *   It has been adapted to fit this project's requirements.
 *
 * Modifications copyright (c) 2025 SelectDB.
 */

package org.apache.doris.common.util;

import com.google.common.base.Preconditions;
import io.grpc.util.CertificateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

public class X509TlsReloadableKeyManager extends X509ExtendedKeyManager {
    private static final Logger LOG = LogManager.getLogger(X509TlsReloadableKeyManager.class);
    // The credential information to be sent to peers to prove our identity.
    private volatile KeyInfo keyInfo;

    public X509TlsReloadableKeyManager() throws CertificateException {
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        if (alias.equals("default")) {
            return this.keyInfo.key;
        }
        return null;
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        if (alias.equals("default")) {
            return Arrays.copyOf(this.keyInfo.certs, this.keyInfo.certs.length);
        }
        return null;
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return new String[] {"default"};
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        return "default";
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        return "default";
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return new String[] {"default"};
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        return "default";
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers,
                                          SSLEngine engine) {
        return "default";
    }

    /**
     * Updates the current cached private key and cert chains.
     *
     * @param key  the private key that is going to be used
     * @param certs  the certificate chain that is going to be used
     */
    public void updateIdentityCredentials(PrivateKey key, X509Certificate[] certs) {
        LOG.info("The key info is update");
        this.keyInfo = new KeyInfo(
                Preconditions.checkNotNull(key, "key"), Preconditions.checkNotNull(certs, "certs"));
    }

    /**
     * Schedules a {@code ScheduledExecutorService} to read certificate chains and private key from
     * the local file paths periodically, and update the cached identity credentials if they are both
     * updated. You must close the returned Closeable before calling this method again or other update
     * methods ({@link X509TlsReloadableKeyManager#updateIdentityCredentials}, {@link
     * X509TlsReloadableKeyManager#updateIdentityCredentialsFromFile(File, File, String)}).
     * Before scheduling the task, the method synchronously executes {@code  readAndUpdate} once. The
     * minimum refresh period of 1 minute is enforced.
     *
     * @param certFile  the file on disk holding the certificate chain
     * @param keyFile  the file on disk holding the private key
     * @param keyPassword the password of keyFile
     * @param period the period between successive read-and-update executions
     * @param unit the time unit of the initialDelay and period parameters
     * @param executor the executor service we use to read and update the credentials
     * @return an object that caller should close when the file refreshes are not needed
     */
    public Closeable updateIdentityCredentialsFromFile(File keyFile, File certFile,
            String keyPassword, long period, TimeUnit unit, ScheduledExecutorService executor)
            throws IOException, GeneralSecurityException {
        CertificateManager.FileWatcherState keyState = new CertificateManager.FileWatcherState();
        CertificateManager.FileWatcherState certState = new CertificateManager.FileWatcherState();
        UpdateResult result = this.readAndUpdate(keyFile, certFile, keyPassword, keyState, certState);
        if (!result.success) {
            executor.shutdown();
            throw new GeneralSecurityException("Files were unmodified before their initial update. Probably a bug.");
        }
        if (period > 0) {
            final ScheduledFuture<?> future = executor.scheduleWithFixedDelay(
                new LoadFilePathExecution(keyFile, certFile, keyPassword, keyState, certState),
                    period, period, unit);
            return () -> {
                future.cancel(false);
                executor.shutdown();
            };
        }
        LOG.warn("SSL certificate reload thread cancle because certificate_reload_interval_s<=0");
        executor.shutdown();
        return () -> {};
    }

    /**
     * Updates certificate chains and the private key from the local file paths.
     *
     * @param certFile  the file on disk holding the certificate chain
     * @param keyFile  the file on disk holding the private key
     * @param keyPassword the password of keyFile
     */
    public void updateIdentityCredentialsFromFile(File keyFile, File certFile, String keyPassword)
            throws IOException, GeneralSecurityException {
        CertificateManager.FileWatcherState keyState = new CertificateManager.FileWatcherState();
        CertificateManager.FileWatcherState certState = new CertificateManager.FileWatcherState();
        UpdateResult newResult =
                this.readAndUpdate(keyFile, certFile, keyPassword, keyState, certState);
        if (!newResult.success) {
            throw new GeneralSecurityException("Files were unmodified before their initial update. Probably a bug.");
        }
    }

    /**
     * Reads the private key and certificates specified in the path locations. Updates {@code key} and
     * {@code cert} if both of their modified time changed since last read.
     *
     * @param certFile  the file on disk holding the certificate chain
     * @param keyFile  the file on disk holding the private key
     * @param keyPassword the password of keyFile
     * @param keyState the time when the private key file is modified during last execution
     * @param certState the time when the certificate chain file is modified during last execution
     * @return the result of this update execution
     */
    private UpdateResult readAndUpdate(File keyFile, File certFile,
            String keyPassword, CertificateManager.FileWatcherState keyState,
            CertificateManager.FileWatcherState certState)
            throws IOException, GeneralSecurityException {
        CertificateManager.FileWatcherState.Snapshot certSnapshot = certState.snapshot();
        CertificateManager.FileWatcherState.Snapshot keySnapshot = keyState.snapshot();

        int checkNum = 0;
        if (CertificateManager.checkCertificateFile(certFile, certState, "certificate", null)) {
            checkNum |= 1;
        }
        if (CertificateManager.checkCertificateFile(keyFile, keyState, "private key", null)) {
            checkNum |= 1 << 1;
        }

        if ((checkNum & 1) != 1) {
            return new UpdateResult(false, keyState.getLastModified(), certState.getLastModified());
        }

        try (FileInputStream certInputStream = new FileInputStream(certFile)) {
            char[] passwordChars = keyPassword == null ? null : keyPassword.toCharArray();
            PrivateKey key = CertificateManager.loadPrivateKey(keyFile.getAbsolutePath(), passwordChars);
            X509Certificate[] certs = CertificateUtils.getX509Certificates(certInputStream);
            this.updateIdentityCredentials(key, certs);
            return new UpdateResult(true, keyState.getLastModified(), certState.getLastModified());
        } catch (GeneralSecurityException | IOException e) {
            certSnapshot.restore(certState);
            keySnapshot.restore(keyState);
            throw e;
        }
    }


    private class LoadFilePathExecution implements Runnable {
        File keyFile;
        File certFile;
        String keyPassword;
        CertificateManager.FileWatcherState keyState;
        CertificateManager.FileWatcherState certState;

        public LoadFilePathExecution(File keyFile, File certFile, String keyPassword,
                                     CertificateManager.FileWatcherState keyState,
                                     CertificateManager.FileWatcherState certState) {
            this.keyFile = keyFile;
            this.certFile = certFile;
            this.keyPassword = keyPassword;
            this.keyState = keyState;
            this.certState = certState;
        }

        public void run() {
            try {
                X509TlsReloadableKeyManager.this.readAndUpdate(this.keyFile, this.certFile,
                        this.keyPassword, this.keyState, this.certState);
            } catch (GeneralSecurityException | IOException e) {
                LOG.error("Failed refreshing private key and certificate chain from files. Using previous ones", e);
            }

        }
    }

    private static class UpdateResult {
        boolean success;
        long keyTime;
        long certTime;

        public UpdateResult(boolean success, long keyTime, long certTime) {
            this.success = success;
            this.keyTime = keyTime;
            this.certTime = certTime;
        }
    }

    private static class KeyInfo {
        final PrivateKey key;
        final X509Certificate[] certs;

        public KeyInfo(PrivateKey key, X509Certificate[] certs) {
            this.key = key;
            this.certs = certs;
        }
    }

    /**
     * Mainly used to avoid throwing IO Exceptions in java.io.Closeable.
     */
    public interface Closeable extends java.io.Closeable {
        void close();
    }
}

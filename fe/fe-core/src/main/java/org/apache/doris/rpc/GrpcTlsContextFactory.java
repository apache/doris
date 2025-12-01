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

package org.apache.doris.rpc;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.X509TlsReloadableKeyManager;
import org.apache.doris.common.util.X509TlsReloadableTrustManager;

import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

/**
 * Provides a shared gRPC {@link SslContext} backed by reloadable key/trust managers so that
 * the FE can hot-reload TLS certificates without rebuilding every gRPC channel.
 */
public final class GrpcTlsContextFactory {
    private static final Logger LOG = LogManager.getLogger(GrpcTlsContextFactory.class);

    // TODO: use CertificateManager to manage a gloable key manager and trustmanager
    private static X509TlsReloadableKeyManager keyManager;
    private static X509TlsReloadableTrustManager trustManager;
    private static X509TlsReloadableKeyManager.Closeable keyManagerCloseable;
    private static X509TlsReloadableTrustManager.Closeable trustManagerCloseable;

    private GrpcTlsContextFactory() {}

    public static SslContext newClientContext() {
        if (!Config.enable_tls) {
            throw new IllegalStateException("TLS is disabled while attempting to build gRPC SSL context");
        }
        initTlsManager();
        return buildClientContext();
    }

    private static SslContext buildClientContext() {
        try {
            return GrpcSslContexts.forClient()
                    .keyManager(keyManager)
                    .trustManager(trustManager)
                    .build();
        } catch (SSLException e) {
            throw new RuntimeException("Failed to build reloadable gRPC SSL context", e);
        }
    }

    private static void initTlsManager() {
        if (keyManager != null && trustManager != null) {
            return;
        }
        try {
            trustManager = X509TlsReloadableTrustManager.newBuilder()
                    .setVerification(
                            X509TlsReloadableTrustManager.Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION)
                    .build();
            trustManagerCloseable = trustManager.updateTrustCredentials(
                    new File(Config.tls_ca_certificate_path),
                    Config.tls_cert_refresh_interval_seconds,
                    TimeUnit.SECONDS,
                    newSingleThreadScheduler("GrpcTls-TrustManager"));

            keyManager = new X509TlsReloadableKeyManager();
            keyManagerCloseable = keyManager.updateIdentityCredentialsFromFile(
                    new File(Config.tls_private_key_path),
                    new File(Config.tls_certificate_path),
                    Config.tls_private_key_password,
                    Config.tls_cert_refresh_interval_seconds,
                    TimeUnit.SECONDS,
                    newSingleThreadScheduler("GrpcTls-KeyManager"));

            LOG.info("Initialized reloadable TLS managers for gRPC clients, refresh interval {}s",
                    Config.tls_cert_refresh_interval_seconds);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize reloadable TLS for gRPC clients", e);
        }
    }

    private static ScheduledExecutorService newSingleThreadScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    public static void shutdown() {
        if (trustManagerCloseable != null) {
            trustManagerCloseable.close();
            trustManagerCloseable = null;
        }
        if (keyManagerCloseable != null) {
            keyManagerCloseable.close();
            keyManagerCloseable = null;
        }
        trustManager = null;
        keyManager = null;
    }
}

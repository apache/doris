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

package org.apache.doris.mysql;

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.X509TlsReloadableKeyManager;
import org.apache.doris.common.util.X509TlsReloadableTrustManager;
import org.apache.doris.qe.ConnectScheduler;
import org.apache.doris.service.FrontendOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * mysql protocol implementation based on nio.
 */
public class MysqlServer {
    private static final Logger LOG = LogManager.getLogger(MysqlServer.class);

    private int port;
    private volatile boolean running;

    private XnioWorker xnioWorker;

    private AcceptListener acceptListener;

    private AcceptingChannel<StreamConnection> server;

    // default task service.
    private ExecutorService taskService = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
            Config.max_mysql_service_task_threads_num, "mysql-nio-pool", true);

    private static X509TlsReloadableTrustManager trustManager;
    private static X509TlsReloadableKeyManager keyManager;
    private static Closeable trustManagerCloseable;
    private static Closeable keyManagerCloseable;

    public MysqlServer(int port, ConnectScheduler connectScheduler) {
        this.port = port;
        this.xnioWorker = Xnio.getInstance().createWorkerBuilder()
                .setWorkerName("doris-mysql-nio")
                .setWorkerIoThreads(Config.mysql_service_io_threads_num)
                .setExternalExecutorService(taskService).build();
        // connectScheduler only used for idle check.
        this.acceptListener = new AcceptListener(connectScheduler);
    }

    // start MySQL protocol service
    // return true if success, otherwise false
    public boolean start() {
        try {
            if (FrontendOptions.isBindIPV6()) {
                server = xnioWorker.createStreamConnectionServer(new InetSocketAddress("::0", port), acceptListener,
                    OptionMap.create(Options.TCP_NODELAY, true, Options.BACKLOG, Config.mysql_nio_backlog_num));

            } else {
                server = xnioWorker.createStreamConnectionServer(new InetSocketAddress(port), acceptListener,
                    OptionMap.create(Options.TCP_NODELAY, true, Options.BACKLOG, Config.mysql_nio_backlog_num));

            }
            initTlsManager();
            server.resumeAccepts();
            running = true;
            LOG.info("Open mysql server success on {}", port);
            return true;
        } catch (IOException e) {
            LOG.warn("Open MySQL network service failed.", e);
            return false;
        }
    }

    private void initTlsManager() {
        if (!Config.enable_tls) {
            return;
        }
        try {
            if (trustManager == null) {
                trustManager = X509TlsReloadableTrustManager.newBuilder()
                    .setVerification(X509TlsReloadableTrustManager.Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION)
                    .build();
                trustManagerCloseable = trustManager.updateTrustCredentials(
                        new File(Config.tls_ca_certificate_path),
                        Config.tls_cert_refresh_interval_seconds, TimeUnit.SECONDS,
                        Executors.newSingleThreadScheduledExecutor(r -> {
                            Thread t = new Thread(r, "MysqlSSL-TrustManager");
                            t.setDaemon(true);
                            return t;
                        })
                );
            }

            if (keyManager == null) {
                keyManager = new X509TlsReloadableKeyManager();
                keyManagerCloseable = keyManager.updateIdentityCredentialsFromFile(
                        new File(Config.tls_private_key_path), new File(Config.tls_certificate_path),
                        Config.tls_private_key_password, Config.tls_cert_refresh_interval_seconds, TimeUnit.SECONDS,
                        Executors.newSingleThreadScheduledExecutor(r -> {
                            Thread t = new Thread(r, "MysqlSSL-KeyManager");
                            t.setDaemon(true);
                            return t;
                        })
                );
            }
        } catch (IOException | GeneralSecurityException e) {
            LOG.error("Failed to initialize MySQL TLS Manager", e);
            throw new RuntimeException("Failed to initialize MySQL TLS Manager: " + e.getMessage(), e);
        }

    }

    private void stopTlsManager() {
        if (trustManagerCloseable != null) {
            try {
                trustManagerCloseable.close();
                LOG.debug("Closed TrustManager certificate reload task");
            } catch (Exception e) {
                LOG.warn("Failed to close TrustManager closeable", e);
            }
        }
        if (keyManagerCloseable != null) {
            try {
                keyManagerCloseable.close();
                LOG.debug("Closed KeyManager certificate reload task");
            } catch (Exception e) {
                LOG.warn("Failed to close KeyManager closeable", e);
            }
        }
    }

    public static X509TlsReloadableKeyManager getKeyManager() {
        if (!Config.enable_tls) {
            LOG.warn("Not set enable_tls");
            throw new RuntimeException("Not set enable_tls");
        }
        return keyManager;
    }

    public static X509TlsReloadableTrustManager getTrustManager() {
        if (!Config.enable_tls) {
            LOG.warn("Not set enable_tls");
            throw new RuntimeException("Not set enable_tls");
        }
        return trustManager;
    }

    public void stop() {
        if (running) {
            running = false;
            // close server channel, make accept throw exception
            try {
                server.close();
            } catch (IOException e) {
                LOG.warn("close server channel failed.", e);
            }
            stopTlsManager();
        }
    }
}

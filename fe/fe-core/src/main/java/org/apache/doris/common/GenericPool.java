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

package org.apache.doris.common;

import org.apache.doris.common.util.X509TlsReloadableKeyManager;
import org.apache.doris.thrift.TNetworkAddress;

import io.grpc.util.AdvancedTlsX509TrustManager;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;

public class GenericPool<VALUE extends org.apache.thrift.TServiceClient>  {
    private static final Logger LOG = LogManager.getLogger(GenericPool.class);
    private GenericKeyedObjectPool<TNetworkAddress, VALUE> pool;
    private String className;
    private int timeoutMs;
    private boolean isNonBlockingIO;

    // Global shared TLS managers for certificate hot-reload
    private static volatile X509TlsReloadableKeyManager sharedKeyManager;
    private static volatile AdvancedTlsX509TrustManager sharedTrustManager;
    private static volatile Closeable keyManagerCloseable;
    private static volatile Closeable trustManagerCloseable;
    private static final Object tlsInitLock = new Object();

    public GenericPool(String className, GenericKeyedObjectPoolConfig config, int timeoutMs, boolean isNonBlockingIO) {
        this.className = "org.apache.doris.thrift." + className + "$Client";
        ThriftClientFactory factory = new ThriftClientFactory();
        pool = new GenericKeyedObjectPool<>(factory, config);
        this.timeoutMs = timeoutMs;
        this.isNonBlockingIO = isNonBlockingIO;

        // Initialize global TLS managers
        if (Config.enable_tls) {
            initGlobalTlsManagers();
        }
    }

    /**
     * Initialize global shared TLS managers with certificate hot-reload support.
     * This method is thread-safe and will only initialize once.
     */
    private static void initGlobalTlsManagers() {
        if (sharedKeyManager == null || sharedTrustManager == null) {
            synchronized (tlsInitLock) {
                // Double-check locking
                if (sharedKeyManager == null || sharedTrustManager == null) {
                    try {
                        // Initialize TrustManager with auto-reload
                        sharedTrustManager = AdvancedTlsX509TrustManager.newBuilder()
                            .setVerification(
                                    AdvancedTlsX509TrustManager.Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION)
                            .build();
                        trustManagerCloseable = sharedTrustManager.updateTrustCredentialsFromFile(
                                new File(Config.tls_ca_certificate_path),
                                Config.tls_cert_refresh_interval_seconds, TimeUnit.SECONDS,
                                Executors.newSingleThreadScheduledExecutor(r -> {
                                    Thread t = new Thread(r, "GenericPool-TrustManager");
                                    t.setDaemon(true);
                                    return t;
                                })
                        );

                        // Initialize KeyManager with auto-reload
                        sharedKeyManager = new X509TlsReloadableKeyManager();
                        keyManagerCloseable = sharedKeyManager.updateIdentityCredentialsFromFile(
                                new File(Config.tls_private_key_path),
                                new File(Config.tls_certificate_path),
                                Config.tls_private_key_password,
                                Config.tls_cert_refresh_interval_seconds, TimeUnit.SECONDS,
                                Executors.newSingleThreadScheduledExecutor(r -> {
                                    Thread t = new Thread(r, "GenericPool-KeyManager");
                                    t.setDaemon(true);
                                    return t;
                                })
                        );

                        LOG.info("Initialized global TLS managers with certificate hot-reload for GenericPool");
                    } catch (Exception e) {
                        LOG.error("Failed to initialize global TLS managers", e);
                        throw new RuntimeException("Failed to initialize TLS for GenericPool", e);
                    }
                }
            }
        }
    }

    /**
     * Stop certificate monitoring tasks. Should be called when shutting down.
     */
    public static void stopCertificateMonitoring() {
        synchronized (tlsInitLock) {
            if (trustManagerCloseable != null) {
                try {
                    trustManagerCloseable.close();
                    LOG.info("Stopped GenericPool TrustManager certificate monitoring");
                } catch (Exception e) {
                    LOG.warn("Failed to close GenericPool TrustManager certificate monitoring", e);
                }
                trustManagerCloseable = null;
            }
            if (keyManagerCloseable != null) {
                try {
                    keyManagerCloseable.close();
                    LOG.info("Stopped GenericPool KeyManager certificate monitoring");
                } catch (Exception e) {
                    LOG.warn("Failed to close GenericPool KeyManager certificate monitoring", e);
                }
                keyManagerCloseable = null;
            }
        }
    }

    public GenericPool(String className, GenericKeyedObjectPoolConfig config, int timeoutMs) {
        this(className, config, timeoutMs, false);
    }

    public boolean reopen(VALUE object, int timeoutMs) {
        boolean ok = true;
        object.getOutputProtocol().getTransport().close();
        try {
            object.getOutputProtocol().getTransport().open();
            // transport.open() doesn't set timeout, Maybe the timeoutMs change.
            // here we cannot set timeoutMs for TFramedTransport, just skip it
            if (!isNonBlockingIO) {
                TSocket socket = (TSocket) object.getOutputProtocol().getTransport();
                socket.setTimeout(timeoutMs);
            }
        } catch (TTransportException e) {
            ok = false;
        }
        return ok;
    }

    public boolean reopen(VALUE object) {
        boolean ok = true;
        object.getOutputProtocol().getTransport().close();
        try {
            object.getOutputProtocol().getTransport().open();
        } catch (TTransportException e) {
            LOG.warn("reopen error", e);
            ok = false;
        }
        return ok;
    }

    public void clearPool(TNetworkAddress addr) {
        pool.clear(addr);
    }

    public boolean peak(VALUE object) {
        return object.getOutputProtocol().getTransport().peek();
    }

    public VALUE borrowObject(TNetworkAddress address) throws Exception {
        return pool.borrowObject(address);
    }

    public VALUE borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
        VALUE value = pool.borrowObject(address);
        // here we cannot set timeoutMs for TFramedTransport, just skip it
        if (!isNonBlockingIO) {
            TSocket socket = (TSocket) (value.getOutputProtocol().getTransport());
            socket.setTimeout(timeoutMs);
        }
        return value;
    }

    public void returnObject(TNetworkAddress address, VALUE object) {
        if (address == null || object == null) {
            return;
        }
        pool.returnObject(address, object);
    }

    public void invalidateObject(TNetworkAddress address, VALUE object) {
        if (address == null || object == null) {
            return;
        }
        try {
            pool.invalidateObject(address, object);
        } catch (Exception e) {
            LOG.warn("failed to invalidate object. address: {}", address.toString(), e);
        }
    }

    private class ThriftClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, VALUE> {

        private Object newInstance(String className, TProtocol protocol) throws Exception {
            Class newoneClass = Class.forName(className);
            Constructor cons = newoneClass.getConstructor(TProtocol.class);
            return cons.newInstance(protocol);
        }

        @Override
        public VALUE create(TNetworkAddress key) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("before create socket hostname={} key.port={} timeoutMs={}",
                        key.hostname, key.port, timeoutMs);
            }

            TSocket clientSocket;
            if (Config.enable_tls) {
                // Use global shared TLS managers for certificate hot-reload
                SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(new KeyManager[]{ sharedKeyManager },
                                new TrustManager[]{ sharedTrustManager },
                                null);
                SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket(key.hostname, key.port);
                socket.setSoTimeout(timeoutMs);
                clientSocket = new TSocket(socket);
            } else {
                clientSocket = new TSocket(key.hostname, key.port, timeoutMs);
            }

            TTransport transport = isNonBlockingIO
                    ? new TFramedTransport(new TSocket(key.hostname, key.port, timeoutMs))
                    : clientSocket;
            if (!Config.enable_tls) {
                transport.open();
            }
            TProtocol protocol = new TBinaryProtocol(transport);
            VALUE client = (VALUE) newInstance(className, protocol);
            return client;
        }

        @Override
        public PooledObject<VALUE> wrap(VALUE client) {
            return new DefaultPooledObject<VALUE>(client);
        }

        @Override
        public boolean validateObject(TNetworkAddress key, PooledObject<VALUE> p) {
            boolean isOpen = p.getObject().getOutputProtocol().getTransport().isOpen();
            if (LOG.isDebugEnabled()) {
                LOG.debug("isOpen={}", isOpen);
            }
            return isOpen;
        }

        @Override
        public void destroyObject(TNetworkAddress key, PooledObject<VALUE> p) {
            // InputProtocol and OutputProtocol have the same reference in OurCondition
            if (p.getObject().getOutputProtocol().getTransport().isOpen()) {
                p.getObject().getOutputProtocol().getTransport().close();
            }
        }
    }
}

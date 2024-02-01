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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

public class MysqlSslContext {

    private static final Logger LOG = LogManager.getLogger(MysqlSslContext.class);
    private SSLEngine sslEngine;
    private SSLContext sslContext;
    private String protocol;
    private ByteBuffer serverAppData;
    private static final String keyStoreFile = Config.mysql_ssl_default_server_certificate;
    private static final String trustStoreFile = Config.mysql_ssl_default_ca_certificate;
    private static final String caCertificatePassword = Config.mysql_ssl_default_ca_certificate_password;
    private static final String serverCertificatePassword = Config.mysql_ssl_default_server_certificate_password;
    private static final String trustStoreType = Config.ssl_trust_store_type;
    private ByteBuffer serverNetData;
    private ByteBuffer clientAppData;
    private ByteBuffer clientNetData;

    public MysqlSslContext(String protocol) {
        this.protocol = protocol;
    }

    public void init() {
        initSslContext();
        initSslEngine();
    }

    private void initSslContext() {
        try {
            KeyStore ks = KeyStore.getInstance(trustStoreType);
            KeyStore ts = KeyStore.getInstance(trustStoreType);

            char[] serverPassword = serverCertificatePassword.toCharArray();
            char[] caPassword = caCertificatePassword.toCharArray();

            try (InputStream stream = Files.newInputStream(Paths.get(keyStoreFile))) {
                ks.load(stream, serverPassword);
            }
            try (InputStream stream = Files.newInputStream(Paths.get(trustStoreFile))) {
                ts.load(stream, caPassword);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, serverPassword);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            sslContext = SSLContext.getInstance(protocol);
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException | IOException
                 | CertificateException | UnrecoverableKeyException e) {
            LOG.fatal("Failed to initialize SSL because", e);
        }
    }

    private void initSslEngine() {
        sslEngine = sslContext.createSSLEngine();
        // set to server mode
        sslEngine.setUseClientMode(false);
        sslEngine.setEnabledCipherSuites(sslEngine.getSupportedCipherSuites());
        sslEngine.setWantClientAuth(true);
        if (Config.ssl_force_client_auth) {
            sslEngine.setNeedClientAuth(true);
        }
    }

    public SSLEngine getSslEngine() {
        return sslEngine;
    }

    public String getProtocol() {
        return protocol;
    }


    /*
      There may several steps for a successful handshake,
      so it's typical to see the following series of operations:

           client          server          message
           ======          ======          =======
           wrap()          ...             ClientHello
           ...             unwrap()        ClientHello
           ...             wrap()          ServerHello/Certificate
           unwrap()        ...             ServerHello/Certificate
           wrap()          ...             ClientKeyExchange
           wrap()          ...             ChangeCipherSpec
           wrap()          ...             Finished
           ...             unwrap()        ClientKeyExchange
           ...             unwrap()        ChangeCipherSpec
           ...             unwrap()        Finished
           ...             wrap()          ChangeCipherSpec
           ...             wrap()          Finished
           unwrap()        ...             ChangeCipherSpec
           unwrap()        ...             Finished
       reference: https://docs.oracle.com/javase/10/security/java-secure-socket-extension-jsse-reference-guide.htm#JSSEC-GUID-7FCC21CB-158B-440C-B5E4-E4E5A2D7352B
     */
    public boolean sslExchange(MysqlChannel channel) throws Exception {
        // long startTime = System.currentTimeMillis();
        // init data buffer
        initDataBuffer();
        // set channel sslengine.
        channel.setSslEngine(sslEngine);
        // begin handshake.
        sslEngine.beginHandshake();
        while (sslEngine.getHandshakeStatus() != HandshakeStatus.FINISHED
                && sslEngine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
            //            if ((System.currentTimeMillis() - startTime) > 10000) {
            //                throw new Exception("try to establish SSL connection failed, timeout!");
            //            }
            switch (sslEngine.getHandshakeStatus()) {
                case NEED_WRAP:
                    handleNeedWrap(channel);
                    break;
                case NEED_UNWRAP:
                    handleNeedUnwrap(channel);
                    break;
                case NEED_TASK:
                    handleNeedTask();
                    break;
                // Under normal circumstances, the following states will not appear
                case NOT_HANDSHAKING:
                    throw new Exception("impossible HandshakeStatus: " + HandshakeStatus.NOT_HANDSHAKING);
                case FINISHED:
                    throw new Exception("impossible HandshakeStatus: " + HandshakeStatus.FINISHED);
                default:
                    throw new IllegalStateException("invalid HandshakeStatus: " + sslEngine.getHandshakeStatus());
            }
        }
        return true;
    }

    private void initDataBuffer() {
        int appLength = sslEngine.getSession().getApplicationBufferSize();
        int netLength = sslEngine.getSession().getPacketBufferSize();
        serverAppData = clientAppData = ByteBuffer.allocate(appLength);
        serverNetData = clientNetData = ByteBuffer.allocate(netLength);
    }

    private void handleNeedTask() throws Exception {
        Runnable runnable;
        while ((runnable = sslEngine.getDelegatedTask()) != null) {
            runnable.run();
        }
        HandshakeStatus hsStatus = sslEngine.getHandshakeStatus();
        if (hsStatus == HandshakeStatus.NEED_TASK) {
            throw new Exception("handshake shouldn't need additional tasks");
        }
    }

    private void handleNeedWrap(MysqlChannel channel) {
        try {
            while (true) {
                SSLEngineResult sslEngineResult = sslEngine.wrap(serverAppData, serverNetData);
                if (handleWrapResult(sslEngineResult)) {
                    // if wrap normal, send packet.
                    // todo: refactor sendAndFlush.
                    serverNetData.flip();
                    channel.sendAndFlush(serverNetData);
                    serverNetData.clear();
                    break;
                }
                // if BUFFER_OVERFLOW, need to wrap again, so we do nothing.
            }
        } catch (SSLException e) {
            sslEngine.closeOutbound();
        } catch (IOException e) {
            throw new RuntimeException("send failed");
        }
    }

    private void handleNeedUnwrap(MysqlChannel channel) {
        try {
            // todo: refactor readAll.
            clientNetData = channel.fetchOnePacket();
            while (true) {
                SSLEngineResult sslEngineResult = sslEngine.unwrap(clientNetData, clientAppData);
                if (handleUnwrapResult(sslEngineResult)) {
                    clientAppData.clear();
                    break;
                }
                // if BUFFER_OVERFLOW or BUFFER_UNDERFLOW, need to unwrap again, so we do nothing.
            }
        } catch (IOException e) {
            throw new RuntimeException("send failed");
        }
    }


    private boolean handleWrapResult(SSLEngineResult sslEngineResult) throws SSLException {
        switch (sslEngineResult.getStatus()) {
            // normal status.
            case OK:
                return true;
            case CLOSED:
                sslEngine.closeOutbound();
                return true;
            case BUFFER_OVERFLOW:
                // Could attempt to drain the serverNetData buffer of any already obtained
                // data, but we'll just increase it to the size needed.
                ByteBuffer newBuffer = ByteBuffer.allocate(serverNetData.capacity() * 2);
                serverNetData.flip();
                newBuffer.put(serverNetData);
                serverNetData = newBuffer;
                // retry the operation.
                return false;
            // when wrap BUFFER_UNDERFLOW and other status will not appear.
            case BUFFER_UNDERFLOW:
            default:
                throw new IllegalStateException("invalid wrap status: " + sslEngineResult.getStatus());
        }
    }

    private boolean handleUnwrapResult(SSLEngineResult sslEngineResult) {
        switch (sslEngineResult.getStatus()) {
            // normal status.
            case OK:
                return true;
            case CLOSED:
                sslEngine.closeOutbound();
                return true;
            case BUFFER_OVERFLOW:
                // Could attempt to drain the clientAppData buffer of any already obtained
                // data, but we'll just increase it to the size needed.
                ByteBuffer newAppBuffer = ByteBuffer.allocate(clientAppData.capacity() * 2);
                clientAppData.flip();
                newAppBuffer.put(clientAppData);
                clientAppData = newAppBuffer;
                // retry the operation.
                return false;
            case BUFFER_UNDERFLOW:
                int netSize = sslEngine.getSession().getPacketBufferSize();
                // Resize buffer if needed.
                if (netSize > clientAppData.capacity()) {
                    ByteBuffer newNetBuffer = ByteBuffer.allocateDirect(netSize);
                    clientNetData.flip();
                    newNetBuffer.put(clientNetData);
                    clientNetData = newNetBuffer;
                }
                // Obtain more inbound network data for clientNetData,
                // then retry the operation.
                return false;
            default:
                throw new IllegalStateException("invalid wrap status: " + sslEngineResult.getStatus());
        }

    }
}

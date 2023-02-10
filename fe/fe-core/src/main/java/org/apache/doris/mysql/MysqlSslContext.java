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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

public class MysqlSslContext {

    private static final Logger LOG = LogManager.getLogger(MysqlSslContext.class);
    private SSLEngine sslEngine;
    private SSLContext sslContext;
    private String protocol;
    private String[] ciphersuites;

    private ByteBuffer serverAppDate;
    private ByteBuffer serverNetDate;
    private ByteBuffer clientAppDate;
    private ByteBuffer clientNetDate;

    public MysqlSslContext(String protocol, String[] ciphersuites) {
        protocol = protocol;
        ciphersuites = ciphersuites;
        initSslContext();
        initSslEngine();
    }

    private void initSslContext() {
        try {
            sslContext = SSLContext.getInstance(protocol);
            sslContext.init(null, null, null);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            LOG.error("Failed to initialize SSL because", e);
        }
    }

    private void initSslEngine() {
        sslEngine = sslContext.createSSLEngine();
        // set to server mode
        sslEngine.setUseClientMode(false);
        sslEngine.setEnabledCipherSuites(ciphersuites);
    }

    public SSLEngine getSslEngine() {
        return sslEngine;
    }

    public String getProtocol() {
        return protocol;
    }

    public String[] getCiphersuites() {
        return ciphersuites;
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
            long startTime = System.currentTimeMillis();
        // begin handshake.
        sslEngine.beginHandshake();
        while (sslEngine.getHandshakeStatus() != HandshakeStatus.FINISHED
                && sslEngine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
            if ((System.currentTimeMillis() - startTime) > 10000) {
                throw new Exception("try to establish SSL connection failed, timeout!");
            }
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

    private void handleNeedTask() throws Exception {
        Runnable runnable;
        while ((runnable = sslEngine.getDelegatedTask()) != null) {
            runnable.run();
        }
        HandshakeStatus hsStatus = sslEngine.getHandshakeStatus();
        if (hsStatus == HandshakeStatus.NEED_TASK) {
            throw new Exception(
                    "handshake shouldn't need additional tasks");
        }
    }

    private void handleNeedWrap(MysqlChannel channel) {
        // todo
        serverAppDate.clear();
        try {
            SSLEngineResult sslEngineResult = sslEngine.wrap(serverAppDate, serverNetDate);
            if(handleWrapResult(sslEngineResult)){
                channel.sendAndFlush(serverNetDate);
                serverNetDate.clear();
            }else{
                // todo
            }
        } catch (SSLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException("send failed");
        }
    }

    private void handleNeedUnwrap(MysqlChannel channel) {
        // todo
    }

    private boolean handleWrapResult(SSLEngineResult sslEngineResult){
            switch(sslEngineResult.getStatus()){
                case OK:
                    return true;
                case CLOSED:
                    // todo
                case BUFFER_OVERFLOW:
                case BUFFER_UNDERFLOW:
                default:
                    throw new IllegalStateException("invalid wrap status: " + sslEngineResult.getStatus());
            }
    }
}

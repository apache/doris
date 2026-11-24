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

package org.apache.doris.maxcompute;

import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMaxComputeBlockIdRequest;
import org.apache.doris.thrift.TMaxComputeBlockIdResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * FE thrift client used by MaxCompute writer runtime code in BE's embedded JVM.
 */
class MaxComputeFeClient implements AutoCloseable {
    static final String FE_HOST = "fe_host";
    static final String FE_PORT = "fe_port";
    static final String FE_RPC_TIMEOUT_MS = "fe_rpc_timeout_ms";
    static final String FE_THRIFT_SERVER_TYPE = "fe_thrift_server_type";

    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeFeClient.class);
    private static final int FETCH_BLOCK_ID_MAX_RETRY_TIMES = 3;
    private static final long FETCH_BLOCK_ID_RETRY_SLEEP_MS = 10L;
    private static final long FETCH_BLOCK_ID_LENGTH = 1L;
    private static final int DEFAULT_FE_RPC_TIMEOUT_MS = 60000;
    private static final String THREADED_SELECTOR = "THREADED_SELECTOR";
    private static final String THREAD_POOL = "THREAD_POOL";

    private final int rpcTimeoutMs;
    private final String thriftServerType;
    private final RpcExecutor rpcExecutor;
    private final long retrySleepMs;
    private TNetworkAddress masterAddress;

    static MaxComputeFeClient create(Map<String, String> params) {
        String host = requireParam(params, FE_HOST);
        int port = Integer.parseInt(requireParam(params, FE_PORT));
        int timeoutMs = Integer.parseInt(params.getOrDefault(FE_RPC_TIMEOUT_MS,
                String.valueOf(DEFAULT_FE_RPC_TIMEOUT_MS)));
        String serverType = params.getOrDefault(FE_THRIFT_SERVER_TYPE, THREAD_POOL);
        return new MaxComputeFeClient(new TNetworkAddress(host, port), timeoutMs, serverType,
                new ReusableRpcExecutor(params),
                FETCH_BLOCK_ID_RETRY_SLEEP_MS);
    }

    MaxComputeFeClient(TNetworkAddress masterAddress, int rpcTimeoutMs, String thriftServerType,
            RpcExecutor rpcExecutor, long retrySleepMs) {
        this.masterAddress = copyAddress(Objects.requireNonNull(masterAddress, "masterAddress"));
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.thriftServerType = thriftServerType == null ? THREAD_POOL : thriftServerType;
        this.rpcExecutor = Objects.requireNonNull(rpcExecutor, "rpcExecutor");
        this.retrySleepMs = retrySleepMs;
    }

    long requestBlockId(long txnId, String writeSessionId) throws IOException {
        if (txnId <= 0) {
            throw new IOException("invalid MaxCompute txn_id for block_id allocation: " + txnId);
        }
        if (writeSessionId == null || writeSessionId.isEmpty()) {
            throw new IOException("empty MaxCompute write_session_id for block_id allocation");
        }

        TMaxComputeBlockIdRequest request = buildBlockIdRequest(txnId, writeSessionId);
        return callWithMasterRedirect(
                "allocate MaxCompute block_id",
                client -> client.getMaxComputeBlockIdRange(request),
                (result, requestAddress, retryTimes) ->
                        handleBlockIdResult(result, requestAddress, retryTimes, txnId, writeSessionId));
    }

    @Override
    public synchronized void close() {
        rpcExecutor.close();
    }

    private synchronized <T, R> R callWithMasterRedirect(String operation, FeCall<T> call,
            ResponseHandler<T, R> handler)
            throws IOException {
        validateAddress(masterAddress);

        Exception lastException = null;
        for (int retryTimes = 0; retryTimes < FETCH_BLOCK_ID_MAX_RETRY_TIMES; retryTimes++) {
            TNetworkAddress requestAddress = copyAddress(masterAddress);
            T result;
            try {
                result = rpcExecutor.call(requestAddress, rpcTimeoutMs, useFramedTransport(), call);
            } catch (Exception e) {
                lastException = e;
                rpcExecutor.close();
                LOG.warn("Failed to " + operation + ", rpc failure, retry_time="
                        + retryTimes + ", fe=" + formatAddress(requestAddress), e);
                sleepBeforeRetry();
                continue;
            }

            try {
                return handler.handle(result, requestAddress, retryTimes);
            } catch (NotMasterException e) {
                masterAddress = copyAddress(e.masterAddress);
                lastException = e;
                rpcExecutor.close();
                sleepBeforeRetry();
            }
        }

        throw new IOException("failed to " + operation + " from FE", lastException);
    }

    private long handleBlockIdResult(TMaxComputeBlockIdResult result, TNetworkAddress requestAddress, int retryTimes,
            long txnId, String writeSessionId) throws IOException, NotMasterException {
        if (result == null || !result.isSetStatus()) {
            throw new IOException("failed to allocate MaxCompute block_id from FE, missing status in response, "
                    + "txn_id=" + txnId + ", write_session_id=" + writeSessionId);
        }

        TStatus status = result.getStatus();
        TStatusCode code = status.getStatusCode();
        if (code == null) {
            throw new IOException("failed to allocate MaxCompute block_id from FE, missing status code, "
                    + "txn_id=" + txnId + ", write_session_id=" + writeSessionId);
        }
        if (code == TStatusCode.NOT_MASTER) {
            if (!result.isSetMasterAddress()) {
                throw new IOException("failed to allocate MaxCompute block_id from FE, missing master address "
                        + "in NOT_MASTER response, txn_id=" + txnId + ", write_session_id=" + writeSessionId);
            }
            LOG.warn("Failed to allocate MaxCompute block_id, requested non-master FE@"
                    + formatAddress(requestAddress) + ", switch to FE@" + formatAddress(result.getMasterAddress())
                    + ", retry_time=" + retryTimes + ", txn_id=" + txnId
                    + ", write_session_id=" + writeSessionId);
            throw new NotMasterException(result.getMasterAddress());
        }

        if (code != TStatusCode.OK) {
            throw new IOException("failed to allocate MaxCompute block_id from FE, status="
                    + statusErrorMessage(status) + ", txn_id=" + txnId
                    + ", write_session_id=" + writeSessionId);
        }

        if (!result.isSetStart()) {
            throw new IOException("failed to allocate MaxCompute block_id from FE, missing start in response, "
                    + "txn_id=" + txnId + ", write_session_id=" + writeSessionId);
        }
        if (!result.isSetLength() || result.getLength() != FETCH_BLOCK_ID_LENGTH) {
            throw new IOException("failed to allocate MaxCompute block_id from FE, expected length=1 but got "
                    + result.getLength() + ", txn_id=" + txnId + ", write_session_id=" + writeSessionId);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Allocated MaxCompute block_id from FE@" + formatAddress(requestAddress)
                    + ", txn_id=" + txnId + ", write_session_id=" + writeSessionId
                    + ", block_id=" + result.getStart());
        }
        return result.getStart();
    }

    private static TMaxComputeBlockIdRequest buildBlockIdRequest(long txnId, String writeSessionId) {
        TMaxComputeBlockIdRequest request = new TMaxComputeBlockIdRequest();
        request.setTxnId(txnId);
        request.setWriteSessionId(writeSessionId);
        request.setLength(FETCH_BLOCK_ID_LENGTH);
        return request;
    }

    private boolean useFramedTransport() {
        return THREADED_SELECTOR.equalsIgnoreCase(thriftServerType);
    }

    private void sleepBeforeRetry() throws IOException {
        try {
            Thread.sleep(retrySleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while retrying MaxCompute block_id allocation", e);
        }
    }

    private static void validateAddress(TNetworkAddress address) throws IOException {
        if (address.getHostname() == null || address.getHostname().isEmpty() || address.getPort() <= 0) {
            throw new IOException("invalid FE address for MaxCompute block_id allocation: "
                    + formatAddress(address));
        }
    }

    private static String statusErrorMessage(TStatus status) {
        List<String> errorMsgs = status.getErrorMsgs();
        if (errorMsgs == null || errorMsgs.isEmpty()) {
            return status.getStatusCode().name();
        }
        return status.getStatusCode().name() + ": " + String.join("; ", errorMsgs);
    }

    private static String requireParam(Map<String, String> params, String key) {
        String value = params.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("required property '" + key + "'.");
        }
        return value;
    }

    private static TNetworkAddress copyAddress(TNetworkAddress address) {
        return new TNetworkAddress(address.getHostname(), address.getPort());
    }

    private static boolean sameAddress(TNetworkAddress left, TNetworkAddress right) {
        return left != null && right != null
                && Objects.equals(left.getHostname(), right.getHostname())
                && left.getPort() == right.getPort();
    }

    private static String formatAddress(TNetworkAddress address) {
        if (address == null) {
            return "null";
        }
        return address.getHostname() + ":" + address.getPort();
    }

    interface RpcExecutor {
        <T> T call(TNetworkAddress address, int timeoutMs, boolean useFramedTransport,
                FeCall<T> call) throws Exception;

        default void close() {
        }
    }

    interface FeCall<T> {
        T call(FrontendService.Client client) throws Exception;
    }

    private interface ResponseHandler<T, R> {
        R handle(T result, TNetworkAddress requestAddress, int retryTimes) throws IOException, NotMasterException;
    }

    private static class ReusableRpcExecutor implements RpcExecutor {
        private final boolean tls;
        private final String certificate;
        private final String privateKey;
        private final String ca;
        private final String handshakeTimeout;
        private TNetworkAddress connectedAddress;
        private boolean connectedFramedTransport;
        private TTransport transport;
        private FrontendService.Client client;

        ReusableRpcExecutor(Map<String, String> params) {
            // The native writer supplies its own node policy; this plugin cannot load FE-core Config.
            tls = Boolean.parseBoolean(params.getOrDefault("fe_rpc_tls_enabled", "false"));
            certificate = params.get("fe_rpc_tls_certificate_path");
            privateKey = params.get("fe_rpc_tls_private_key_path");
            ca = params.get("fe_rpc_tls_ca_certificate_path");
            handshakeTimeout = params.get("fe_rpc_connect_timeout_ms");
        }

        private TTransport createTransport(TNetworkAddress address, int timeoutMs, boolean framed)
                throws TTransportException {
            if (!tls) {
                TSocket socket = new TSocket(address.getHostname(), address.getPort(), timeoutMs);
                return framed ? new TFramedTransport(socket) : socket;
            }
            Socket tcp = null;
            SSLSocket ssl = null;
            boolean opened = false;
            try {
                long configured = Long.parseLong(handshakeTimeout);
                if (configured <= 0 || configured > Integer.MAX_VALUE || framed) {
                    throw new IllegalArgumentException("Private Thrift TLS requires bounded blocking IO");
                }
                int bound = timeoutMs > 0 ? Math.min(timeoutMs, (int) configured) : (int) configured;
                JdkSslContext context = (JdkSslContext) SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                        .trustManager(new File(ca)).keyManager(new File(certificate), new File(privateKey)).build();
                tcp = new Socket();
                tcp.connect(new InetSocketAddress(address.getHostname(), address.getPort()), bound);
                tcp.setTcpNoDelay(true);
                ssl = (SSLSocket) context.context().getSocketFactory()
                        .createSocket(tcp, address.getHostname(), address.getPort(), true);
                ssl.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.3"});
                ssl.setSoTimeout(bound);
                SSLParameters parameters = ssl.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ssl.setSSLParameters(parameters);
                ssl.startHandshake();
                ssl.setSoTimeout(timeoutMs);
                TSocket socket = new TSocket(ssl);
                opened = true;
                return socket;
            } catch (Exception failure) {
                // Parser and handshake exceptions may include identity paths or PEM input.
                throw new TTransportException(TTransportException.NOT_OPEN, "Private RPC TLS connection failed");
            } finally {
                if (!opened) {
                    Socket failed = ssl == null ? tcp : ssl;
                    if (failed != null) {
                        try {
                            failed.close();
                        } catch (IOException ignored) {
                            // Never publish an unauthenticated or partially initialized connection.
                        }
                    }
                }
            }
        }

        @Override
        public synchronized <T> T call(TNetworkAddress address, int timeoutMs, boolean useFramedTransport,
                FeCall<T> call) throws Exception {
            ensureConnected(address, timeoutMs, useFramedTransport);
            try {
                return call.call(client);
            } catch (Exception e) {
                close();
                throw e;
            }
        }

        @Override
        public synchronized void close() {
            if (transport != null) {
                transport.close();
            }
            transport = null;
            client = null;
            connectedAddress = null;
        }

        private void ensureConnected(TNetworkAddress address, int timeoutMs, boolean useFramedTransport)
                throws Exception {
            if (client != null && transport != null && transport.isOpen()
                    && connectedFramedTransport == useFramedTransport
                    && sameAddress(connectedAddress, address)) {
                return;
            }

            close();
            TTransport newTransport = createTransport(address, timeoutMs, useFramedTransport);
            try {
                if (!newTransport.isOpen()) {
                    newTransport.open();
                }
                transport = newTransport;
                client = new FrontendService.Client(new TBinaryProtocol(transport));
                connectedAddress = copyAddress(address);
                connectedFramedTransport = useFramedTransport;
            } catch (Exception e) {
                newTransport.close();
                throw e;
            }
        }
    }

    private static class NotMasterException extends Exception {
        private final TNetworkAddress masterAddress;

        NotMasterException(TNetworkAddress masterAddress) {
            super("not master, master=" + formatAddress(masterAddress));
            this.masterAddress = copyAddress(masterAddress);
        }
    }
}

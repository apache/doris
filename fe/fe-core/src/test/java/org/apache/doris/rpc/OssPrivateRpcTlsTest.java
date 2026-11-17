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
import org.apache.doris.common.OssThriftClientTransportProvider;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.tls.server.OssFeServerStarterProvider;
import org.apache.doris.tls.server.ServerStarter;

import com.google.protobuf.StringValue;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

class OssPrivateRpcTlsTest {
    private static final MethodDescriptor<StringValue, StringValue> PROBE = MethodDescriptor
            .<StringValue, StringValue>newBuilder().setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("tls.Probe/echo")
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance())).build();
    private static final StringValue PAYLOAD = StringValue.of("fixture-key-payload");

    @TempDir
    Path certificates;
    private boolean enableTls;
    private String excluded;
    private String mode;
    private String certificate;
    private String privateKey;
    private String ca;
    private int refreshInterval;
    private int connectTimeout;

    @BeforeEach
    void configureFixture() throws Exception {
        enableTls = Config.enable_tls;
        excluded = Config.tls_excluded_protocols;
        mode = Config.tls_verify_mode;
        certificate = Config.tls_certificate_path;
        privateKey = Config.tls_private_key_path;
        ca = Config.tls_ca_certificate_path;
        refreshInterval = Config.tls_cert_refresh_interval_seconds;
        connectTimeout = Config.thrift_rpc_connect_timeout_ms;
        Path script = Path.of(System.getProperty("user.dir"))
                .resolve("../../docker/thirdparties/docker-compose/iceberg/encryption/generate-tls-fixture.sh")
                .normalize();
        Process generation = new ProcessBuilder("bash", script.toString(), certificates.toString())
                .redirectErrorStream(true).redirectOutput(certificates.resolve("generation.log").toFile()).start();
        Assertions.assertTrue(generation.waitFor(60, TimeUnit.SECONDS));
        Assertions.assertEquals(0, generation.exitValue());
        Config.enable_tls = true;
        Config.tls_cert_refresh_interval_seconds = 0;
        Config.tls_verify_mode = "verify_fail_if_no_peer_cert";
        Config.tls_excluded_protocols = "http,mysql,arrowflight,bdbje";
        Config.tls_certificate_path = certificates.resolve("fe.pem").toString();
        Config.tls_private_key_path = certificates.resolve("fe.key").toString();
        Config.tls_ca_certificate_path = certificates.resolve("ca.pem").toString();
    }

    @AfterEach
    void restoreConfig() {
        Config.enable_tls = enableTls;
        Config.tls_excluded_protocols = excluded;
        Config.tls_verify_mode = mode;
        Config.tls_certificate_path = certificate;
        Config.tls_private_key_path = privateKey;
        Config.tls_ca_certificate_path = ca;
        Config.tls_cert_refresh_interval_seconds = refreshInterval;
        Config.thrift_rpc_connect_timeout_ms = connectTimeout;
    }

    @Test
    void fragmentChannelUsesMutualTlsAndPreservesAuthorityAfterIpResolution() throws Exception {
        try (Peer peer = peer("be")) {
            Assertions.assertEquals(PAYLOAD, invoke(peer, "localhost"));
        }
    }

    @Test
    void privateKeyCapabilityTracksBothProtocolPolicies() {
        OssBackendServiceClientChannelProvider channels = new OssBackendServiceClientChannelProvider();
        OssThriftClientTransportProvider transports = new OssThriftClientTransportProvider();
        Assertions.assertTrue(channels.supportsPrivateFileKeys());
        Assertions.assertTrue(transports.supportsPrivateFileKeys());
        Assertions.assertFalse(new OssBackendServiceClientChannelProvider() {}.supportsPrivateFileKeys());
        Assertions.assertFalse(new OssThriftClientTransportProvider() {}.supportsPrivateFileKeys());
        Config.tls_excluded_protocols = "brpc";
        Assertions.assertFalse(channels.supportsPrivateFileKeys());
        Assertions.assertTrue(transports.supportsPrivateFileKeys());
        Config.tls_excluded_protocols = "thrift";
        Assertions.assertTrue(channels.supportsPrivateFileKeys());
        Assertions.assertFalse(transports.supportsPrivateFileKeys());
        Config.tls_excluded_protocols = "";
        Config.tls_verify_mode = "verify_peer";
        Assertions.assertFalse(channels.supportsPrivateFileKeys());
        Assertions.assertFalse(transports.supportsPrivateFileKeys());
    }

    @Test
    void fragmentChannelRejectsAnotherCa() throws Exception {
        try (Peer peer = peer("untrusted")) {
            Assertions.assertThrows(StatusRuntimeException.class, () -> invoke(peer, "localhost"));
        }
    }

    @Test
    void fragmentChannelRejectsAnotherHostOnTheTrustedCa() throws Exception {
        try (Peer peer = peer("wrong-name")) {
            Assertions.assertThrows(StatusRuntimeException.class, () -> invoke(peer, "localhost"));
        }
    }

    @Test
    void tlsConfigurationCannotFallBackToAPlaintextPeer() throws Exception {
        try (Peer peer = peer(null)) {
            Assertions.assertThrows(StatusRuntimeException.class, () -> invoke(peer, "localhost"));
        }
    }

    @Test
    void defaultOffStillSupportsPlaintext() throws Exception {
        Config.enable_tls = false;
        try (Peer peer = peer(null)) {
            Assertions.assertEquals(PAYLOAD, invoke(peer, "localhost"));
        }
    }

    @Test
    void lazySplitServerRequiresMutualTlsOnTheActualThriftListener() throws Exception {
        String previousType = Config.thrift_server_type;
        Config.thrift_server_type = "THREAD_POOL";
        try (ServerSocket reservation = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            int port = reservation.getLocalPort();
            ServerStarter starter = new OssFeServerStarterProvider().createThriftServerStarter(reservation);
            try {
                Assertions.assertFalse(starter.supportsPrivateFileKeys());
                Assertions.assertDoesNotThrow(starter::start);
                Assertions.assertTrue(starter.supportsPrivateFileKeys());
                for (String identity : new String[] {"be", "untrusted", "be", "", "be"}) {
                    SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK)
                            .trustManager(certificates.resolve("ca.pem").toFile());
                    if (!identity.isEmpty()) {
                        builder.keyManager(certificates.resolve(identity + ".pem").toFile(),
                                certificates.resolve(identity + ".key").toFile());
                    }
                    JdkSslContext client = (JdkSslContext) builder.build();
                    try (SSLSocket socket = (SSLSocket) client.context().getSocketFactory()
                            .createSocket("localhost", port)) {
                        socket.setSoTimeout(3000);
                        // TLS 1.2 reports a rejected client identity during the handshake itself.
                        socket.setEnabledProtocols(new String[] {"TLSv1.2"});
                        SSLParameters parameters = socket.getSSLParameters();
                        parameters.setEndpointIdentificationAlgorithm("HTTPS");
                        socket.setSSLParameters(parameters);
                        if (identity.equals("be")) {
                            Assertions.assertDoesNotThrow(socket::startHandshake);
                            Assertions.assertTrue(socket.getSession().getPeerCertificates().length > 0);
                        } else {
                            Assertions.assertThrows(IOException.class, socket::startHandshake);
                        }
                    }
                }
            } finally {
                starter.stop();
                Assertions.assertFalse(starter.supportsPrivateFileKeys());
            }
        } finally {
            Config.thrift_server_type = previousType;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void thriftClientReopensWithTlsForBothTransportTypes(boolean framed) throws Exception {
        JdkSslContext context = (JdkSslContext) SslContextBuilder.forServer(
                certificates.resolve("fe.pem").toFile(), certificates.resolve("fe.key").toFile())
                .sslProvider(SslProvider.JDK).trustManager(certificates.resolve("ca.pem").toFile())
                .clientAuth(ClientAuth.REQUIRE).build();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (SSLServerSocket server = (SSLServerSocket) context.context().getServerSocketFactory()
                .createServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            server.setNeedClientAuth(true);
            server.setSoTimeout(10000);
            Future<?> peer = worker.submit(() -> {
                for (int round = 0; round < 2; round++) {
                    try (SSLSocket accepted = (SSLSocket) server.accept()) {
                        accepted.setSoTimeout(3000);
                        TTransport transport = new TSocket(accepted);
                        if (framed) {
                            transport = new TFramedTransport(transport);
                        }
                        byte[] payload = new byte[1];
                        transport.readAll(payload, 0, payload.length);
                        transport.write(payload);
                        transport.flush();
                    }
                }
                return null;
            });
            try (TTransport client = new OssThriftClientTransportProvider().createTransport(
                    new TNetworkAddress("localhost", server.getLocalPort()), 3000, framed)) {
                TSocket socket = framed ? (TSocket) ((TFramedTransport) client).getInnerTransport() : (TSocket) client;
                for (int round = 0; round < 2; round++) {
                    socket.setTimeout(3000);
                    client.open();
                    Assertions.assertInstanceOf(SSLSocket.class, socket.getSocket());
                    Assertions.assertEquals(3000, socket.getSocket().getSoTimeout());
                    byte[] payload = {(byte) (40 + round)};
                    client.write(payload);
                    client.flush();
                    byte[] actual = new byte[1];
                    client.readAll(actual, 0, actual.length);
                    Assertions.assertArrayEquals(payload, actual);
                    client.close();
                }
                peer.get(10, TimeUnit.SECONDS);
            } finally {
                server.close();
                worker.shutdownNow();
                Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
            }
        } finally {
            worker.shutdownNow();
        }
    }

    @Test
    void idleThriftPeerCannotKeepAnUnauthenticatedWorker() throws Exception {
        String previousType = Config.thrift_server_type;
        Config.thrift_server_type = "THREAD_POOL";
        Config.thrift_rpc_connect_timeout_ms = 500;
        try (ServerSocket reservation = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            int port = reservation.getLocalPort();
            ServerStarter starter = new OssFeServerStarterProvider().createThriftServerStarter(reservation);
            try {
                starter.start();
                try (Socket idlePeer = new Socket("127.0.0.1", port)) {
                    idlePeer.setSoTimeout(5000);
                    // JSSE may send a TLS alert before closing. Require EOF within the read deadline.
                    Assertions.assertTrue(idlePeer.getInputStream().readNBytes(64).length < 64);
                }
                try (TTransport trusted = new OssThriftClientTransportProvider().createTransport(
                        new TNetworkAddress("localhost", port), 3000, false)) {
                    Assertions.assertDoesNotThrow(trusted::open);
                }
            } finally {
                starter.stop();
            }
        } finally {
            Config.thrift_server_type = previousType;
        }
    }

    @Test
    void clientHandshakeIsBoundedWhenRpcReadsAreUnlimited() throws Exception {
        Config.thrift_rpc_connect_timeout_ms = 500;
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch releasePeer = new CountDownLatch(1);
        try (ServerSocket peer = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
                TTransport client = new OssThriftClientTransportProvider().createTransport(
                        new TNetworkAddress("localhost", peer.getLocalPort()), 0, false)) {
            Future<?> accepted = workers.submit(() -> {
                try (Socket socket = peer.accept()) {
                    Assertions.assertTrue(releasePeer.await(10, TimeUnit.SECONDS));
                }
                return null;
            });
            try {
                Future<TTransportException> connection = workers.submit(
                        () -> Assertions.assertThrows(TTransportException.class, client::open));
                Assertions.assertEquals(TTransportException.TIMED_OUT,
                        connection.get(3, TimeUnit.SECONDS).getType());
            } finally {
                releasePeer.countDown();
                accepted.get(5, TimeUnit.SECONDS);
            }
        } finally {
            releasePeer.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private StringValue invoke(Peer peer, String host) throws Exception {
        ManagedChannel channel = new OssBackendServiceClientChannelProvider().createChannel(
                new TNetworkAddress(host, peer.server.getPort()), "127.0.0.1", Runnable::run);
        try {
            return ClientCalls.blockingUnaryCall(channel, PROBE,
                    CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS), PAYLOAD);
        } finally {
            channel.shutdownNow();
            Assertions.assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private Peer peer(String identity) throws Exception {
        NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).addService(
                ServerServiceDefinition.builder("tls.Probe").addMethod(PROBE,
                        ServerCalls.asyncUnaryCall((request, observer) -> {
                            observer.onNext(request);
                            observer.onCompleted();
                        })).build());
        if (identity != null) {
            builder.sslContext(GrpcSslContexts.forServer(certificates.resolve(identity + ".pem").toFile(),
                    certificates.resolve(identity + ".key").toFile())
                    .trustManager(certificates.resolve("ca.pem").toFile()).clientAuth(ClientAuth.REQUIRE).build());
        }
        return new Peer(builder.build().start());
    }

    private static final class Peer implements AutoCloseable {
        final Server server;

        Peer(Server server) {
            this.server = server;
        }

        @Override
        public void close() throws Exception {
            server.shutdownNow();
            Assertions.assertTrue(server.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}

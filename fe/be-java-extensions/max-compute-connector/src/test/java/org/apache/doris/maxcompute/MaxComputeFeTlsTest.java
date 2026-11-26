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

import com.aliyun.odps.table.write.TableBatchWriteSession;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

class MaxComputeFeTlsTest {
    @TempDir
    static Path certificates;

    @BeforeAll
    static void generateIdentities() throws Exception {
        Path script = Path.of(System.getProperty("user.dir"))
                .resolve("../../../regression-test/suites/rpc_p0/tls/generate-tls-fixture.sh").normalize();
        Process process = new ProcessBuilder("bash", script.toString(), certificates.toString())
                .redirectErrorStream(true).redirectOutput(certificates.resolve("generation.log").toFile()).start();
        try {
            Assertions.assertTrue(process.waitFor(60, TimeUnit.SECONDS));
            Assertions.assertEquals(0, process.exitValue());
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void initialAllocationAndWriterRotationUseAuthenticatedCallbacks() throws Exception {
        for (boolean preallocated : new boolean[] {false, true}) {
            try (FrontendFixture server = new FrontendFixture(true)) {
                Map<String, String> options = params(server, true);
                options.put("endpoint", "http://localhost");
                options.put("project", "test_project");
                options.put("table", "test_table");
                options.put("txn_id", "1");
                options.put("write_session_id", "session");
                if (preallocated) {
                    // A preallocated first block must not conceal the callback needed on rotation.
                    options.put("block_id", "99");
                }
                MaxComputeJniWriter writer = new MaxComputeJniWriter(1, options);
                Field session = MaxComputeJniWriter.class.getDeclaredField("writeSession");
                session.setAccessible(true);
                session.set(writer, Mockito.mock(TableBatchWriteSession.class));
                Field connection = MaxComputeJniWriter.class.getDeclaredField("feClient");
                connection.setAccessible(true);
                try (MaxComputeFeClient writerClient = (MaxComputeFeClient) connection.get(writer)) {
                    Method initial = MaxComputeJniWriter.class.getDeclaredMethod("resolveInitialBlockId");
                    initial.setAccessible(true);
                    Assertions.assertEquals(preallocated ? 99L : 1L, initial.invoke(writer));
                    Method rotate = MaxComputeJniWriter.class.getDeclaredMethod("rotateCurrentBatchWriter");
                    rotate.setAccessible(true);
                    rotate.invoke(writer);
                    Field block = MaxComputeJniWriter.class.getDeclaredField("currentBlockId");
                    block.setAccessible(true);
                    Assertions.assertEquals(preallocated ? 1L : 2L, block.getLong(writer));
                }
                Assertions.assertEquals(preallocated ? 1L : 2L, server.calls.get());
            }
        }
    }

    @Test
    void disabledTlsKeepsPlaintextCallbacks() throws Exception {
        try (FrontendFixture server = new FrontendFixture(false);
                MaxComputeFeClient client = MaxComputeFeClient.create(params(server, false))) {
            Assertions.assertEquals(1, client.requestBlockId(1, "session"));
        }
    }

    @Test
    void untrustedClientCannotAllocateBlocks() throws Exception {
        try (FrontendFixture server = new FrontendFixture(true)) {
            Map<String, String> options = params(server, true);
            options.put("fe_rpc_tls_certificate_path", certificates.resolve("untrusted.pem").toString());
            options.put("fe_rpc_tls_private_key_path", certificates.resolve("untrusted.key").toString());
            try (MaxComputeFeClient client = MaxComputeFeClient.create(options)) {
                Assertions.assertThrows(IOException.class, () -> client.requestBlockId(1, "session"));
            }
            Assertions.assertEquals(0, server.calls.get());
        }
    }

    @Test
    void enabledTlsNeverFallsBackToAPlaintextFrontend() throws Exception {
        try (FrontendFixture server = new FrontendFixture(false);
                MaxComputeFeClient client = MaxComputeFeClient.create(params(server, true))) {
            Assertions.assertThrows(IOException.class, () -> client.requestBlockId(1, "session"));
            Assertions.assertEquals(0, server.calls.get());
        }
    }

    @Test
    void trustedCertificateForAnotherHostCannotAllocateBlocks() throws Exception {
        try (FrontendFixture server = new FrontendFixture(true, "wrong-name");
                MaxComputeFeClient client = MaxComputeFeClient.create(params(server, true))) {
            Assertions.assertThrows(IOException.class, () -> client.requestBlockId(1, "session"));
            Assertions.assertEquals(0, server.calls.get());
        }
    }

    @Test
    void redirectToAnotherFrontendRetainsClientAuthentication() throws Exception {
        try (FrontendFixture master = new FrontendFixture(true);
                FrontendFixture follower = new FrontendFixture(true)) {
            follower.redirect = new TNetworkAddress("localhost", master.listener.getLocalPort());
            try (MaxComputeFeClient client = MaxComputeFeClient.create(params(follower, true))) {
                Assertions.assertEquals(1, client.requestBlockId(1, "session"));
                Assertions.assertEquals(1, master.calls.get());
                Assertions.assertEquals(1, follower.calls.get());
            }
        }
    }

    @Test
    void missingClientIdentityFailsWithoutLeakingItsPath() throws Exception {
        try (FrontendFixture server = new FrontendFixture(true)) {
            Map<String, String> options = params(server, true);
            String missingKey = certificates.resolve("missing-private-key").toString();
            options.put("fe_rpc_tls_private_key_path", missingKey);
            try (MaxComputeFeClient client = MaxComputeFeClient.create(options)) {
                IOException failure = Assertions.assertThrows(IOException.class,
                        () -> client.requestBlockId(1, "session"));
                Assertions.assertFalse(failure.getCause().toString().contains(missingKey));
                Assertions.assertEquals(0, server.calls.get());
            }
        }
    }

    @Test
    void idleTcpPeerCannotMakeAuthenticationUnlimited() throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 10, InetAddress.getLoopbackAddress())) {
            Map<String, String> options = params(listener.getLocalPort(), true);
            options.put("fe_rpc_timeout_ms", "0");
            options.put("fe_rpc_connect_timeout_ms", "100");
            try (MaxComputeFeClient client = MaxComputeFeClient.create(options)) {
                Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5),
                        () -> Assertions.assertThrows(IOException.class, () -> client.requestBlockId(1, "session")));
            }
        }
    }

    @Test
    void nativeCompatiblePkcs1PrivateKeyCanAuthenticate() throws Exception {
        Path traditional = certificates.resolve("be-pkcs1.key");
        try (PemReader reader = new PemReader(Files.newBufferedReader(certificates.resolve("be.key")));
                PemWriter writer = new PemWriter(Files.newBufferedWriter(traditional))) {
            PemObject input = reader.readPemObject();
            // OpenSSL versions differ in whether genrsa writes PKCS#1 or PKCS#8 by default.
            byte[] encoded = "RSA PRIVATE KEY".equals(input.getType()) ? input.getContent()
                    : PrivateKeyInfo.getInstance(input.getContent()).parsePrivateKey().toASN1Primitive().getEncoded();
            writer.writeObject(new PemObject("RSA PRIVATE KEY", encoded));
        }
        Files.setPosixFilePermissions(traditional, PosixFilePermissions.fromString("rw-------"));
        try (FrontendFixture server = new FrontendFixture(true)) {
            Map<String, String> options = params(server, true);
            options.put("fe_rpc_tls_private_key_path", traditional.toString());
            try (MaxComputeFeClient client = MaxComputeFeClient.create(options)) {
                Assertions.assertEquals(1, Assertions.assertDoesNotThrow(() -> client.requestBlockId(1, "session")));
            }
        }
    }

    private static Map<String, String> params(FrontendFixture server, boolean tls) {
        return params(server.listener.getLocalPort(), tls);
    }

    private static Map<String, String> params(int port, boolean tls) {
        Map<String, String> params = new HashMap<>();
        params.put("fe_host", "localhost");
        params.put("fe_port", String.valueOf(port));
        params.put("fe_rpc_timeout_ms", "1000");
        params.put("fe_rpc_tls_enabled", String.valueOf(tls));
        params.put("fe_rpc_tls_certificate_path", certificates.resolve("be.pem").toString());
        params.put("fe_rpc_tls_private_key_path", certificates.resolve("be.key").toString());
        params.put("fe_rpc_tls_ca_certificate_path", certificates.resolve("ca.pem").toString());
        params.put("fe_rpc_connect_timeout_ms", "500");
        return params;
    }

    private static final class FrontendFixture implements AutoCloseable {
        private final ServerSocket listener;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Future<?> task;
        private final AtomicLong calls = new AtomicLong();
        private volatile TNetworkAddress redirect;
        private volatile Socket active;

        FrontendFixture(boolean tls) throws Exception {
            this(tls, "fe");
        }

        FrontendFixture(boolean tls, String identity) throws Exception {
            if (tls) {
                JdkSslContext context = (JdkSslContext) SslContextBuilder.forServer(
                        certificates.resolve(identity + ".pem").toFile(),
                        certificates.resolve(identity + ".key").toFile())
                        .sslProvider(SslProvider.JDK).trustManager(certificates.resolve("ca.pem").toFile())
                        .clientAuth(ClientAuth.REQUIRE).build();
                listener = context.context().getServerSocketFactory()
                        .createServerSocket(0, 10, InetAddress.getLoopbackAddress());
                ((SSLServerSocket) listener).setNeedClientAuth(true);
            } else {
                listener = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
            }
            FrontendService.Iface handler = Mockito.mock(FrontendService.Iface.class);
            Mockito.when(handler.getMaxComputeBlockIdRange(Mockito.any(TMaxComputeBlockIdRequest.class)))
                    .thenAnswer(call -> {
                        long next = calls.incrementAndGet();
                        return redirect == null
                                ? new TMaxComputeBlockIdResult().setStatus(new TStatus(TStatusCode.OK))
                                        .setStart(next).setLength(1)
                                : new TMaxComputeBlockIdResult().setStatus(new TStatus(TStatusCode.NOT_MASTER))
                                        .setMasterAddress(redirect);
                    });
            FrontendService.Processor<FrontendService.Iface> processor = new FrontendService.Processor<>(handler);
            task = executor.submit(() -> {
                while (!listener.isClosed()) {
                    try (Socket socket = listener.accept()) {
                        active = socket;
                        socket.setSoTimeout(2000);
                        if (socket instanceof SSLSocket) {
                            ((SSLSocket) socket).startHandshake();
                        }
                        TBinaryProtocol protocol = new TBinaryProtocol(new TSocket(socket));
                        while (!socket.isClosed()) {
                            processor.process(protocol, protocol);
                        }
                    } catch (Exception rejectedOrClosed) {
                        // Rejected handshakes and client EOF are expected between connections.
                    } finally {
                        active = null;
                    }
                }
            });
        }

        @Override
        public void close() throws Exception {
            listener.close();
            Socket socket = active;
            if (socket != null) {
                socket.close();
            }
            executor.shutdownNow();
            task.get(5, TimeUnit.SECONDS);
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}

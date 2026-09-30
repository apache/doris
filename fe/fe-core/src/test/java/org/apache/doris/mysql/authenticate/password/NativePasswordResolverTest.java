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

package org.apache.doris.mysql.authenticate.password;

import org.apache.doris.common.Config;
import org.apache.doris.mysql.CachingSha2PasswordExchange;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import javax.crypto.Cipher;

class NativePasswordResolverTest {
    private static final Map<String, String> MYSQL_9_CLIENT = ImmutableMap.of("_client_name", "libmysql",
            CachingSha2PasswordExchange.CLIENT_VERSION_ATTR, "9.4.0");
    private static final Map<String, String> MYSQL_8_CLIENT = ImmutableMap.of("_client_name", "libmysql",
            CachingSha2PasswordExchange.CLIENT_VERSION_ATTR, "8.0.44");

    private String previousMode;
    private final MysqlHandshakePacket handshake = new MysqlHandshakePacket(1);
    private final MysqlChannel channel = Mockito.mock(MysqlChannel.class);

    @BeforeEach
    void setUp() {
        previousMode = Config.mysql_caching_sha2_password_clients;
        Mockito.when(channel.getRemoteIp()).thenReturn("127.0.0.1");
    }

    @AfterEach
    void tearDown() {
        Config.mysql_caching_sha2_password_clients = previousMode;
    }

    @Test
    void testMysql9ClientGetsFullAuthenticationOverRsa() throws Exception {
        byte[] nonce = handshake.getAuthPluginData();
        Mockito.when(channel.fetchOnePacket()).thenReturn(
                ByteBuffer.wrap(new byte[32]),                 // its caching_sha2 scramble, ignored
                ByteBuffer.wrap(new byte[] {0x02}),            // asks for the public key
                ByteBuffer.wrap(encryptLikeClient("secret", nonce, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding")));

        NativePassword password = resolve(authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_9_CLIENT));

        Assertions.assertArrayEquals(MysqlPassword.scramble(nonce, "secret"), password.getRemotePasswd());
        Assertions.assertArrayEquals(nonce, password.getRandomString());
        // AuthSwitchRequest, perform-full-authentication, public key
        Mockito.verify(channel, Mockito.times(3)).sendAndFlush(Mockito.any());
    }

    @Test
    void testConnectorJPkcs1PaddingIsAcceptedToo() throws Exception {
        // Connector/J takes the padding from the server version and Doris reports 5.7: PKCS#1 v1.5
        byte[] nonce = handshake.getAuthPluginData();
        Mockito.when(channel.fetchOnePacket()).thenReturn(
                ByteBuffer.wrap(new byte[32]),
                ByteBuffer.wrap(new byte[] {0x02}),
                ByteBuffer.wrap(encryptLikeClient("secret", nonce, "RSA/ECB/PKCS1Padding")));

        NativePassword password = resolve(authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_9_CLIENT));

        Assertions.assertArrayEquals(MysqlPassword.scramble(nonce, "secret"), password.getRemotePasswd());
    }

    @Test
    void testUndecryptableAnswerIsRejectedWithAnError() throws Exception {
        Mockito.when(channel.fetchOnePacket()).thenReturn(
                ByteBuffer.wrap(new byte[32]),
                ByteBuffer.wrap(new byte[] {0x02}),
                ByteBuffer.wrap(new byte[256]));
        Mockito.when(channel.getSerializer()).thenReturn(MysqlSerializer.newInstance());
        ConnectContext context = Mockito.mock(ConnectContext.class);
        QueryState state = new QueryState();
        Mockito.when(context.getState()).thenReturn(state);
        Mockito.when(context.getMysqlChannel()).thenReturn(channel);

        Assertions.assertFalse(new NativePasswordResolver().resolvePassword(context, channel,
                MysqlSerializer.newInstance(), authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_9_CLIENT),
                handshake).isPresent());
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, state.getStateType());
        // AuthSwitchRequest, perform-full-authentication, public key, the error packet
        Mockito.verify(channel, Mockito.times(4)).sendAndFlush(Mockito.any());
    }

    @Test
    void testMysql9ClientSendsThePlaintextOverTls() throws Exception {
        byte[] nonce = handshake.getAuthPluginData();
        Mockito.when(channel.fetchOnePacket()).thenReturn(
                ByteBuffer.wrap(new byte[32]),
                ByteBuffer.wrap("secret\0".getBytes(StandardCharsets.UTF_8)));

        NativePassword password = resolve(authPacket(MysqlCapability.SSL_CAPABILITY, MYSQL_9_CLIENT));

        Assertions.assertArrayEquals(MysqlPassword.scramble(nonce, "secret"), password.getRemotePasswd());
        Mockito.verify(channel, Mockito.times(2)).sendAndFlush(Mockito.any());
    }

    @Test
    void testEmptyPasswordNeedsNoFullAuthentication() throws Exception {
        // libmysqlclient answers the switch with the empty string and its terminator: one NUL byte
        Mockito.when(channel.fetchOnePacket()).thenReturn(ByteBuffer.wrap(new byte[] {0}));

        NativePassword password = resolve(authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_9_CLIENT));

        Assertions.assertEquals(0, password.getRemotePasswd().length);
        Mockito.verify(channel, Mockito.times(1)).sendAndFlush(Mockito.any());
    }

    @Test
    void testMysql8ClientIsStillSwitchedToNativePassword() throws Exception {
        byte[] nativeResponse = new byte[20];
        nativeResponse[0] = 7;
        Mockito.when(channel.fetchOnePacket()).thenReturn(ByteBuffer.wrap(nativeResponse));

        NativePassword password = resolve(authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_8_CLIENT));

        Assertions.assertArrayEquals(nativeResponse, password.getRemotePasswd());
        Mockito.verify(channel, Mockito.times(1)).sendAndFlush(Mockito.any());
    }

    @Test
    void testModeNoneSwitchesEveryClientAndModeAllServesEveryClient() throws Exception {
        Config.mysql_caching_sha2_password_clients = "none";
        Assertions.assertFalse(CachingSha2PasswordExchange.serves(CachingSha2PasswordExchange.PLUGIN_NAME,
                MYSQL_9_CLIENT));
        Config.mysql_caching_sha2_password_clients = "all";
        Assertions.assertTrue(CachingSha2PasswordExchange.serves(CachingSha2PasswordExchange.PLUGIN_NAME,
                ImmutableMap.of()));
        Config.mysql_caching_sha2_password_clients = "auto";
        Assertions.assertFalse(CachingSha2PasswordExchange.serves(CachingSha2PasswordExchange.PLUGIN_NAME,
                ImmutableMap.of()));
        Assertions.assertFalse(CachingSha2PasswordExchange.serves("mysql_native_password", MYSQL_9_CLIENT));
        Assertions.assertFalse(CachingSha2PasswordExchange.serves(CachingSha2PasswordExchange.PLUGIN_NAME,
                ImmutableMap.of(CachingSha2PasswordExchange.CLIENT_VERSION_ATTR, "x")));
    }

    @Test
    void testClientClosingTheConnectionMidExchangeYieldsNoRequest() throws Exception {
        Mockito.when(channel.fetchOnePacket()).thenReturn(ByteBuffer.wrap(new byte[32]), (ByteBuffer) null);

        Assertions.assertFalse(new NativePasswordResolver().resolvePassword(Mockito.mock(ConnectContext.class), channel,
                MysqlSerializer.newInstance(), authPacket(MysqlCapability.DEFAULT_CAPABILITY, MYSQL_9_CLIENT),
                handshake).isPresent());
    }

    private NativePassword resolve(MysqlAuthPacket authPacket) throws Exception {
        return (NativePassword) new NativePasswordResolver().resolvePassword(Mockito.mock(ConnectContext.class),
                channel, MysqlSerializer.newInstance(), authPacket, handshake)
                .orElseThrow(() -> new AssertionError("password is required"));
    }

    private static MysqlAuthPacket authPacket(MysqlCapability capability, Map<String, String> attributes) {
        MysqlAuthPacket authPacket = Mockito.mock(MysqlAuthPacket.class);
        Mockito.when(authPacket.getCapability()).thenReturn(capability);
        Mockito.when(authPacket.getPluginName()).thenReturn(CachingSha2PasswordExchange.PLUGIN_NAME);
        Mockito.when(authPacket.getConnectAttributes()).thenReturn(attributes);
        Mockito.when(authPacket.getAuthResponse()).thenReturn(new byte[0]);
        return authPacket;
    }

    // What a client does with the PEM key the server sent: RSA over (password + NUL) XOR nonce
    private static byte[] encryptLikeClient(String password, byte[] nonce, String transformation) throws Exception {
        String pem = CachingSha2PasswordExchange.publicKeyPem()
                .replace("-----BEGIN PUBLIC KEY-----", "").replace("-----END PUBLIC KEY-----", "").replace("\n", "");
        PublicKey key = KeyFactory.getInstance("RSA")
                .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(pem)));
        byte[] plain = (password + "\0").getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < plain.length; i++) {
            plain[i] ^= nonce[i % nonce.length];
        }
        Cipher cipher = Cipher.getInstance(transformation);
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(plain);
    }
}

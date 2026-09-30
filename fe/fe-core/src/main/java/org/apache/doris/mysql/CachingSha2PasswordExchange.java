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

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Map;
import javax.crypto.Cipher;

/**
 * Server side of the caching_sha2_password "full authentication" exchange, which yields the
 * client's plaintext password. Doris keeps only the mysql_native_password hash, so the fast path
 * of the plugin (a SHA-256 scramble) can never be verified here; every client that authenticates
 * with this plugin is taken through full authentication and the password is then checked the
 * native way by the caller.
 *
 * Why this exists: MySQL 9 removed mysql_native_password from its client library's built-in
 * plugins, so a MySQL 9 client asked to switch to it fails with "Authentication plugin
 * 'mysql_native_password' cannot be loaded" before Doris sees a password. Such a client announces
 * caching_sha2_password in its handshake response, with an empty auth response and the connection
 * attribute _client_version.
 *
 * The exchange, after the client's handshake response:
 * <pre>
 *   server: AuthSwitchRequest(caching_sha2_password, nonce)      (the handshake nonce is reused)
 *   client: 32-byte scramble, or empty for an empty password
 *   server: AuthMoreData 0x04 (perform full authentication)
 *   client: over TLS, the NUL-terminated plaintext password;
 *           otherwise 0x02 to ask for the server's RSA public key, or the encrypted password
 *           right away when it already holds the key (--server-public-key-path)
 *   server: AuthMoreData + PEM public key, when asked
 *   client: RSA(password XOR nonce, NUL-terminated), OAEP or PKCS#1 padded
 * </pre>
 * The RSA key pair lives in this process only; a client fetches the public key on every
 * connection, so frontends need not share one.
 */
public class CachingSha2PasswordExchange {
    private static final Logger LOG = LogManager.getLogger(CachingSha2PasswordExchange.class);

    public static final String PLUGIN_NAME = "caching_sha2_password";
    public static final String CLIENT_VERSION_ATTR = "_client_version";

    private static final int AUTH_SWITCH_REQUEST = 0xfe;
    private static final int AUTH_MORE_DATA = 0x01;
    private static final int PERFORM_FULL_AUTHENTICATION = 0x04;
    private static final int REQUEST_PUBLIC_KEY = 0x02;
    private static final int SCRAMBLE_LENGTH = 32;
    // libmysqlclient always encrypts with OAEP. Connector/J picks the padding from the server
    // version it was told, OAEP from 8.0.5 on and PKCS#1 v1.5 before, and Doris reports itself as
    // 5.7, so both are accepted; the padding is only told apart by trying.
    private static final String[] RSA_TRANSFORMATIONS = {"RSA/ECB/OAEPWithSHA-1AndMGF1Padding",
            "RSA/ECB/PKCS1Padding"};

    private static volatile KeyPair keyPair;

    /**
     * Whether a client that announced this plugin is served with it instead of being switched to
     * mysql_native_password, per Config.mysql_caching_sha2_password_clients: "auto" serves the
     * clients that cannot load the native plugin, i.e. libmysqlclient 9 and later, identified by
     * their _client_version connection attribute; "all" serves every client that asks for the
     * plugin; "none" keeps switching every client to the native plugin.
     */
    public static boolean serves(String pluginName, Map<String, String> connectAttributes) {
        if (!PLUGIN_NAME.equals(pluginName)) {
            return false;
        }
        String mode = Config.mysql_caching_sha2_password_clients;
        if ("all".equalsIgnoreCase(mode)) {
            return true;
        }
        if ("none".equalsIgnoreCase(mode)) {
            return false;
        }
        return clientMajorVersion(connectAttributes.get(CLIENT_VERSION_ATTR)) >= 9;
    }

    // "9.4.0" -> 9; anything unparsable -> 0
    static int clientMajorVersion(String clientVersion) {
        if (Strings.isNullOrEmpty(clientVersion)) {
            return 0;
        }
        int end = 0;
        while (end < clientVersion.length() && Character.isDigit(clientVersion.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(clientVersion.substring(0, end));
    }

    /** The client answered with something that is not part of the exchange. */
    public static class Rejected extends Exception {
        Rejected(String message) {
            super(message);
        }
    }

    /**
     * Runs the exchange on the channel and returns the plaintext password, or null when the
     * client closed the connection.
     */
    public static String exchange(MysqlChannel channel, MysqlSerializer serializer, byte[] nonce,
            boolean ssl) throws IOException, Rejected {
        serializer.reset();
        serializer.writeInt1(AUTH_SWITCH_REQUEST);
        serializer.writeNulTerminateString(PLUGIN_NAME);
        serializer.writeBytes(nonce);
        serializer.writeInt1(0);
        channel.sendAndFlush(serializer.toByteBuffer());

        ByteBuffer scramble = channel.fetchOnePacket();
        if (scramble == null) {
            return null;
        }
        if (isEmptyPassword(scramble)) {
            return "";
        }
        if (scramble.remaining() != SCRAMBLE_LENGTH) {
            throw new Rejected("unexpected scramble length " + scramble.remaining());
        }

        serializer.reset();
        serializer.writeInt1(AUTH_MORE_DATA);
        serializer.writeInt1(PERFORM_FULL_AUTHENTICATION);
        channel.sendAndFlush(serializer.toByteBuffer());

        ByteBuffer response = channel.fetchOnePacket();
        if (response == null) {
            return null;
        }
        if (ssl) {
            LOG.debug("caching_sha2_password: full authentication over TLS");
            return new String(MysqlProto.readNulTerminateString(response), StandardCharsets.UTF_8);
        }
        if (response.remaining() == 1 && (response.get(response.position()) & 0xff) == REQUEST_PUBLIC_KEY) {
            serializer.reset();
            serializer.writeInt1(AUTH_MORE_DATA);
            serializer.writeBytes(publicKeyPem().getBytes(StandardCharsets.US_ASCII));
            channel.sendAndFlush(serializer.toByteBuffer());
            response = channel.fetchOnePacket();
            if (response == null) {
                return null;
            }
        }
        byte[] encrypted = new byte[response.remaining()];
        response.get(encrypted);
        LOG.debug("caching_sha2_password: full authentication over RSA");
        return decryptPassword(encrypted, nonce);
    }

    // An empty password is answered with no scramble: libmysqlclient sends a single NUL byte, the
    // empty string with its terminator; nothing more is fetched from such a client.
    private static boolean isEmptyPassword(ByteBuffer scramble) {
        return scramble.remaining() == 0
                || (scramble.remaining() == 1 && scramble.get(scramble.position()) == 0);
    }

    // RSA ciphertext of (password + NUL) XOR nonce, as libmysqlclient and Connector/J send it
    static String decryptPassword(byte[] encrypted, byte[] nonce) throws Rejected {
        byte[] xored = null;
        for (String transformation : RSA_TRANSFORMATIONS) {
            try {
                Cipher cipher = Cipher.getInstance(transformation);
                cipher.init(Cipher.DECRYPT_MODE, keyPair().getPrivate());
                xored = cipher.doFinal(encrypted);
                break;
            } catch (GeneralSecurityException e) {
                LOG.debug("caching_sha2_password: not {}: {}", transformation, e.toString());
            }
        }
        if (xored == null) {
            throw new Rejected("cannot decrypt the password with either RSA padding");
        }
        byte[] plain = new byte[xored.length];
        for (int i = 0; i < xored.length; i++) {
            plain[i] = (byte) (xored[i] ^ nonce[i % nonce.length]);
        }
        int end = plain.length;
        if (end > 0 && plain[end - 1] == 0) {
            end--;
        }
        return new String(plain, 0, end, StandardCharsets.UTF_8);
    }

    public static String publicKeyPem() {
        String base64 = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                .encodeToString(keyPair().getPublic().getEncoded());
        return "-----BEGIN PUBLIC KEY-----\n" + base64 + "\n-----END PUBLIC KEY-----\n";
    }

    private static KeyPair keyPair() {
        if (keyPair == null) {
            synchronized (CachingSha2PasswordExchange.class) {
                if (keyPair == null) {
                    try {
                        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
                        generator.initialize(2048);
                        keyPair = generator.generateKeyPair();
                    } catch (GeneralSecurityException e) {
                        throw new IllegalStateException("cannot generate the RSA key pair", e);
                    }
                }
            }
        }
        return keyPair;
    }
}

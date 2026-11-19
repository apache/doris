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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Test-only fallback for build images without a runnable OpenSSL CLI. */
public final class GenerateTlsFixture {
    private static final String PASSWORD = "fixture-only-password";
    private static final String ALIAS = "identity";
    private static final Path KEYTOOL = Path.of(System.getProperty("java.home"), "bin", "keytool");

    private GenerateTlsFixture() {
    }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[0]);
        Path stores = Files.createTempDirectory(output, ".tls-keystores-");
        List<Path> created = new ArrayList<>();
        try {
            for (String authority : List.of("ca", "untrusted-ca")) {
                Path store = stores.resolve(authority + ".p12");
                created.add(store);
                generate(store, "Doris Test " + authority,
                        "BC:critical=ca:true", "KU:critical=keyCertSign,cRLSign");
                keytool(store, "-exportcert", "-rfc", "-file", output.resolve(authority + ".pem").toString());
                exportKey(store, output.resolve(authority + ".key"));
            }
            for (String peer : List.of("fe", "be", "dns-fe", "wrong-name", "untrusted")) {
                Path store = stores.resolve(peer + ".p12");
                created.add(store);
                generate(store, peer);
                Path request = output.resolve(peer + ".csr");
                keytool(store, "-certreq", "-file", request.toString());
                String authority = peer.equals("untrusted") ? "untrusted-ca" : "ca";
                String san = switch (peer) {
                    case "dns-fe" -> "DNS:localhost";
                    case "wrong-name" -> "DNS:wrong.invalid";
                    default -> "DNS:localhost,IP:127.0.0.1";
                };
                keytool(stores.resolve(authority + ".p12"), "-gencert", "-rfc",
                        "-infile", request.toString(), "-outfile", output.resolve(peer + ".pem").toString(),
                        "-validity", "2", "-sigalg", "SHA256withRSA",
                        "-ext", "BC:critical=ca:false", "-ext", "KU:critical=digitalSignature,keyEncipherment",
                        "-ext", "EKU=serverAuth,clientAuth", "-ext", "SAN=" + san);
                exportKey(store, output.resolve(peer + ".key"));
            }
        } finally {
            // Keep only the PEM identities consumed by tests, not duplicate private-key stores.
            for (Path store : created) {
                Files.deleteIfExists(store);
            }
            Files.delete(stores);
        }
    }

    private static void generate(Path store, String name, String... extensions) throws Exception {
        List<String> args = new ArrayList<>(List.of("-genkeypair", "-keyalg", "RSA", "-keysize", "2048",
                "-sigalg", "SHA256withRSA", "-validity", "2", "-dname", "CN=" + name));
        for (String extension : extensions) {
            args.add("-ext");
            args.add(extension);
        }
        keytool(store, args.toArray(String[]::new));
    }

    private static void keytool(Path store, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(KEYTOOL.toString(), "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", PASSWORD, "-keypass", PASSWORD, "-alias", ALIAS));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).inheritIO().start();
        // Bound each invocation so a stalled keytool cannot hang fixture generation indefinitely.
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor();
            throw new IllegalStateException("TLS fixture keytool timed out");
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("TLS fixture keytool failed: " + args[0]);
        }
    }

    private static void exportKey(Path store, Path output) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(store)) {
            keys.load(input, PASSWORD.toCharArray());
        }
        byte[] key = keys.getKey(ALIAS, PASSWORD.toCharArray()).getEncoded();
        String encoded = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(key);
        Files.writeString(output, "-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n",
                StandardCharsets.US_ASCII);
    }
}

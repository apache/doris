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

package org.apache.doris.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class CertificateManager {
    private static final Logger LOG = LogManager.getLogger(CertificateManager.class);
    private static final int MAX_RETRY_NUM = 30;
    private static final long RETRY_INTERVAL_MS = 1000;

    private CertificateManager() {}

    /**
     * Read file with retry logic for files that might be temporarily unavailable.
     *
     * @param path the file path to read
     * @param label a descriptive label for logging (e.g., "certificate", "private key")
     * @return the file content as byte array
     * @throws IOException if the file cannot be read after retries
     */
    private static byte[] readFileWithRetry(String path, String label) throws IOException {
        int retryCount = 0;
        IOException lastException = null;

        while (retryCount <= MAX_RETRY_NUM) {
            try {
                return Files.readAllBytes(Paths.get(path));
            } catch (IOException e) {
                lastException = e;

                if (retryCount >= MAX_RETRY_NUM) {
                    LOG.warn("Retry too many times, cancel waiting for {} at path: {}", label, path);
                    break;
                }

                LOG.warn("{} read failed: {}, retry {} times for waiting recreate...",
                        label, path, retryCount + 1);
                retryCount++;

                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for {} to be recreated: {}", label, path);
                    throw new IOException("Interrupted while retrying to read " + label, ie);
                }
            }
        }

        if (retryCount > 0) {
            LOG.warn("{} still not available after {} retries: {}", label, retryCount, path);
        }
        throw lastException;
    }

    /**
     * Open FileInputStream with retry logic for files that might be temporarily unavailable.
     *
     * @param path the file path to open
     * @param label a descriptive label for logging (e.g., "certificate", "CA certificate")
     * @return FileInputStream for the file
     * @throws IOException if the file cannot be opened after retries
     */
    private static FileInputStream openFileWithRetry(String path, String label) throws IOException {
        int retryCount = 0;
        IOException lastException = null;

        while (retryCount <= MAX_RETRY_NUM) {
            try {
                return new FileInputStream(path);
            } catch (IOException e) {
                lastException = e;

                if (retryCount >= MAX_RETRY_NUM) {
                    LOG.warn("Retry too many times, cancel waiting for {} at path: {}", label, path);
                    break;
                }

                LOG.warn("{} open failed: {}, retry {} times for waiting recreate...",
                        label, path, retryCount + 1);
                retryCount++;

                try {
                    Thread.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for {} to be recreated: {}", label, path);
                    throw new IOException("Interrupted while retrying to open " + label, ie);
                }
            }
        }

        if (retryCount > 0) {
            LOG.warn("{} still not available after {} retries: {}", label, retryCount, path);
        }
        throw lastException;
    }

    /**
     * Load a PKCS#12 key store containing the provided certificate and private key.
     *
     * @param certPath path to the X.509 certificate file on disk
     * @param keyPath path to the PEM encoded private key file
     * @param password password used to decrypt the private key and protect the key store entry
     * @return a PKCS#12 key store initialized with the certificate and private key
     * @throws Exception if the certificate or key cannot be read or parsed
     */
    public static KeyStore loadKeyStore(String certPath, String keyPath, char[] password) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate cert;
        try (InputStream in = openFileWithRetry(certPath, "Certificate")) {
            cert = cf.generateCertificate(in);
        }

        PrivateKey privateKey = loadPrivateKey(keyPath, password);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("cert", privateKey, password, new Certificate[]{cert});
        return ks;
    }

    /**
     * Load a PKCS#12 trust store that contains a single certificate authority entry.
     *
     * @param caCertPath path to the CA certificate file used to populate the trust store
     * @return a PKCS#12 trust store initialized with the CA certificate
     * @throws Exception if the certificate cannot be read or parsed
     */
    public static KeyStore loadTrustStore(String caCertPath) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate ca;
        try (InputStream in = openFileWithRetry(caCertPath, "CA certificate")) {
            ca = cf.generateCertificate(in);
        }
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", ca);
        return ts;
    }

    /**
     * Load an RSA private key from a PEM file, supporting both encrypted and plaintext encodings.
     *
     * @param keyPath path to the PEM encoded private key file
     * @param password password used when the private key is encrypted (ignored for plaintext keys)
     * @return a decoded RSA private key instance
     * @throws IOException if the key file cannot be read
     * @throws GeneralSecurityException if the key cannot be decrypted or parsed
     */
    public static PrivateKey loadPrivateKey(String keyPath, char []password)
            throws IOException, GeneralSecurityException {
        char []effectivePassword = password == null ? new char[0] : password;
        String pem = new String(readFileWithRetry(keyPath, "Private key"), StandardCharsets.US_ASCII);
        pem = pem.replaceAll("-----BEGIN (.*)-----", "")
                .replaceAll("-----END (.*)-----", "")
                .replaceAll("\\s", "");
        byte[] decoded = Base64.getDecoder().decode(pem);

        try {
            // try parser PKCS#8 with password
            EncryptedPrivateKeyInfo encryptedInfo = new EncryptedPrivateKeyInfo(decoded);
            String cipherAlgorithm = resolveCipherAlgorithm(encryptedInfo);
            Cipher cipher = Cipher.getInstance(cipherAlgorithm);
            PBEKeySpec pbeKeySpec = new PBEKeySpec(effectivePassword);
            SecretKeyFactory skf = SecretKeyFactory.getInstance(cipherAlgorithm);
            Key key = skf.generateSecret(pbeKeySpec);
            cipher.init(Cipher.DECRYPT_MODE, key, encryptedInfo.getAlgParameters());
            PKCS8EncodedKeySpec keySpec = encryptedInfo.getKeySpec(cipher);
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        } catch (IOException e) {
            // private key is plaintext
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        }
    }

    /**
     * Decrypt the private key file and expose an unencrypted PKCS#8 PEM stream for TLS builders.
     */
    public static InputStream decryptPrivateKey(String keyPath, char[] password)
            throws IOException, GeneralSecurityException {
        PrivateKey privateKey = loadPrivateKey(keyPath, password);
        byte[] encoded = privateKey.getEncoded();
        if (encoded == null || encoded.length == 0) {
            throw new GeneralSecurityException("Failed to encode private key");
        }

        // Convert to PEM format
        String base64Encoded = Base64.getEncoder().encodeToString(encoded);
        StringBuilder pemBuilder = new StringBuilder();
        pemBuilder.append("-----BEGIN PRIVATE KEY-----\n");

        // Split base64 string into 64-character lines
        int index = 0;
        while (index < base64Encoded.length()) {
            int endIndex = Math.min(index + 64, base64Encoded.length());
            pemBuilder.append(base64Encoded.substring(index, endIndex));
            pemBuilder.append("\n");
            index = endIndex;
        }

        pemBuilder.append("-----END PRIVATE KEY-----\n");
        return new ByteArrayInputStream(pemBuilder.toString().getBytes(StandardCharsets.US_ASCII));
    }

    private static String resolveCipherAlgorithm(EncryptedPrivateKeyInfo encryptedInfo)
            throws GeneralSecurityException {
        String algorithm = encryptedInfo.getAlgName();
        if (!"PBES2".equalsIgnoreCase(algorithm)) {
            return algorithm;
        }

        AlgorithmParameters parameters = encryptedInfo.getAlgParameters();
        if (parameters == null) {
            return algorithm;
        }

        String candidate = parameters.getAlgorithm();
        if (candidate != null && candidate.startsWith("PBEWith")) {
            return candidate;
        }

        candidate = parameters.toString();
        if (candidate != null) {
            candidate = candidate.trim();
            int whitespace = candidate.indexOf(' ');
            if (whitespace > 0) {
                candidate = candidate.substring(0, whitespace);
            }
            if (candidate.startsWith("PBEWith")) {
                return candidate;
            }
        }

        throw new GeneralSecurityException("Unsupported PBES2 parameters");
    }


    public static final class FileWatcherState {
        private long lastModified = Long.MIN_VALUE;
        private boolean initialized;

        public long getLastModified() {
            return lastModified;
        }

        public boolean isInitialized() {
            return initialized;
        }

        public void update(long value) {
            this.lastModified = value;
            this.initialized = true;
        }

        public void reset() {
            this.initialized = false;
            this.lastModified = Long.MIN_VALUE;
        }

        public Snapshot snapshot() {
            return new Snapshot(lastModified, initialized);
        }

        public static final class Snapshot {
            private final long lastModified;
            private final boolean initialized;

            private Snapshot(long lastModified, boolean initialized) {
                this.lastModified = lastModified;
                this.initialized = initialized;
            }

            public void restore(FileWatcherState state) {
                if (initialized) {
                    state.update(lastModified);
                } else {
                    state.reset();
                }
            }
        }
    }

    public static boolean checkCertificateFile(File file, FileWatcherState state, String fileType,
            BooleanSupplier shouldStop) {
        if (file == null) {
            return false;
        }

        if (!file.exists()) {
            if (state != null && state.isInitialized()) {
                LOG.warn("{} file deleted: {}, waiting for recreation...", fileType, file.getAbsolutePath());
                if (waitForFileRecreation(file, fileType, state, shouldStop)) {
                    return true;
                }
            }
            if (state != null) {
                state.reset();
            }
            return false;
        }

        long currentModified = file.lastModified();
        if (state == null) {
            return false;
        }

        if (!state.isInitialized()) {
            state.update(currentModified);
            LOG.info("{} file write time initialized: {}", fileType, currentModified);
            return true;
        }

        if (currentModified != state.getLastModified()) {
            LOG.info("{} file write time changed: {} -> {}", fileType,
                    state.getLastModified(), currentModified);
            state.update(currentModified);
            return true;
        }

        return false;
    }

    private static boolean waitForFileRecreation(File file, String fileType, FileWatcherState state,
            BooleanSupplier shouldStop) {
        final int waitSeconds = 30;
        for (int waited = 0; waited < waitSeconds; waited++) {
            if (shouldStop != null && shouldStop.getAsBoolean()) {
                return false;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for {} file recreation: {}", fileType,
                        file.getAbsolutePath(), e);
                return false;
            }

            if (file.exists()) {
                long newModified = file.lastModified();
                if (state != null) {
                    state.update(newModified);
                }
                LOG.info("{} file recreated and detected: {}", fileType, file.getAbsolutePath());
                return true;
            }
        }

        LOG.error("{} file was deleted and not recreated within {} seconds, certificate reload cancelled: {}",
                fileType, waitSeconds, file.getAbsolutePath());
        return false;
    }

}

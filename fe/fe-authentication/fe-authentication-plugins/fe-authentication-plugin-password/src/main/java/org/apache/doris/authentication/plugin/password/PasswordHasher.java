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

package org.apache.doris.authentication.plugin.password;

import org.mindrot.jbcrypt.BCrypt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Password hashing utility supporting multiple algorithms.
 *
 * <p>Supported algorithms:
 * <ul>
 *   <li>BCrypt (default, recommended) - adaptive hashing with salt</li>
 *   <li>SHA-256 - fast but less secure, for backward compatibility</li>
 *   <li>PLAIN - no hashing (for testing only, NOT for production)</li>
 * </ul>
 */
public class PasswordHasher {

    private static final String BCRYPT_PREFIX = "$2a$";
    private static final String SHA256_PREFIX = "{SHA256}";
    private static final String PLAIN_PREFIX = "{PLAIN}";
    private static final int BCRYPT_LOG_ROUNDS = 10; // 2^10 iterations

    /**
     * Hash algorithm enumeration.
     */
    public enum Algorithm {
        BCRYPT,
        SHA256,
        PLAIN
    }

    /**
     * Hash a password using the default algorithm (BCrypt).
     *
     * @param plainPassword plain text password
     * @return hashed password with algorithm prefix
     */
    public static String hash(String plainPassword) {
        return hash(plainPassword, Algorithm.BCRYPT);
    }

    /**
     * Hash a password using the specified algorithm.
     *
     * @param plainPassword plain text password
     * @param algorithm hashing algorithm
     * @return hashed password with algorithm prefix
     */
    public static String hash(String plainPassword, Algorithm algorithm) {
        if (plainPassword == null) {
            throw new IllegalArgumentException("plainPassword cannot be null");
        }

        switch (algorithm) {
            case BCRYPT:
                return BCrypt.hashpw(plainPassword, BCrypt.gensalt(BCRYPT_LOG_ROUNDS));
            case SHA256:
                return SHA256_PREFIX + hashSha256(plainPassword);
            case PLAIN:
                return PLAIN_PREFIX + plainPassword;
            default:
                throw new IllegalArgumentException("Unknown algorithm: " + algorithm);
        }
    }

    /**
     * Verify a password against a hashed password.
     * Automatically detects the hashing algorithm from the prefix.
     *
     * @param plainPassword plain text password to verify
     * @param hashedPassword hashed password (with algorithm prefix)
     * @return true if password matches, false otherwise
     */
    public static boolean verify(String plainPassword, String hashedPassword) {
        if (plainPassword == null || hashedPassword == null) {
            return false;
        }

        if (hashedPassword.startsWith(BCRYPT_PREFIX)) {
            return verifyBcrypt(plainPassword, hashedPassword);
        } else if (hashedPassword.startsWith(SHA256_PREFIX)) {
            return verifySha256(plainPassword, hashedPassword);
        } else if (hashedPassword.startsWith(PLAIN_PREFIX)) {
            return verifyPlain(plainPassword, hashedPassword);
        } else {
            // Try BCrypt without prefix for backward compatibility
            if (hashedPassword.startsWith("$2")) {
                return verifyBcrypt(plainPassword, hashedPassword);
            }
            return false;
        }
    }

    /**
     * Detect the hashing algorithm from a hashed password.
     *
     * @param hashedPassword hashed password
     * @return detected algorithm
     */
    public static Algorithm detectAlgorithm(String hashedPassword) {
        if (hashedPassword == null) {
            throw new IllegalArgumentException("hashedPassword cannot be null");
        }

        if (hashedPassword.startsWith(BCRYPT_PREFIX) || hashedPassword.startsWith("$2")) {
            return Algorithm.BCRYPT;
        } else if (hashedPassword.startsWith(SHA256_PREFIX)) {
            return Algorithm.SHA256;
        } else if (hashedPassword.startsWith(PLAIN_PREFIX)) {
            return Algorithm.PLAIN;
        } else {
            throw new IllegalArgumentException("Unknown password hash format: " + hashedPassword);
        }
    }

    /**
     * Check if a password needs rehashing (e.g., algorithm changed or work factor increased).
     *
     * @param hashedPassword current hashed password
     * @param targetAlgorithm target algorithm
     * @return true if rehashing is needed
     */
    public static boolean needsRehash(String hashedPassword, Algorithm targetAlgorithm) {
        if (hashedPassword == null) {
            return true;
        }

        Algorithm currentAlgorithm = detectAlgorithm(hashedPassword);
        if (currentAlgorithm != targetAlgorithm) {
            return true;
        }

        // For BCrypt, check if work factor needs to be increased
        if (currentAlgorithm == Algorithm.BCRYPT) {
            // BCrypt format: $2a$<rounds>$<salt+hash>
            String[] parts = hashedPassword.split("\\$");
            if (parts.length >= 3) {
                try {
                    int currentRounds = Integer.parseInt(parts[2]);
                    return currentRounds < BCRYPT_LOG_ROUNDS;
                } catch (NumberFormatException e) {
                    return true;
                }
            }
        }

        return false;
    }

    // ==================== Private Methods ====================

    private static boolean verifyBcrypt(String plainPassword, String hashedPassword) {
        try {
            return BCrypt.checkpw(plainPassword, hashedPassword);
        } catch (IllegalArgumentException e) {
            // Invalid hash format
            return false;
        }
    }

    private static boolean verifySha256(String plainPassword, String hashedPassword) {
        String expectedHash = hashedPassword.substring(SHA256_PREFIX.length());
        String actualHash = hashSha256(plainPassword);
        return MessageDigest.isEqual(expectedHash.getBytes(), actualHash.getBytes());
    }

    private static boolean verifyPlain(String plainPassword, String hashedPassword) {
        String storedPassword = hashedPassword.substring(PLAIN_PREFIX.length());
        return MessageDigest.isEqual(plainPassword.getBytes(), storedPassword.getBytes());
    }

    private static String hashSha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}

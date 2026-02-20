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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * MySQL password handling utilities.
 *
 * <p>This class implements the MySQL native password authentication algorithm.
 * The algorithm uses SHA-1 hashing and XOR operations to create and verify
 * scrambled passwords without sending the actual password over the network.
 *
 * <h3>Algorithm Overview:</h3>
 * <pre>
 * SERVER:  public_seed = create_random_string()
 *          send(public_seed)
 *
 * CLIENT:  recv(public_seed)
 *          hash_stage1 = sha1("password")
 *          hash_stage2 = sha1(hash_stage1)
 *          reply = xor(hash_stage1, sha1(public_seed, hash_stage2))
 *          send(reply)
 *
 * SERVER:  recv(reply)
 *          hash_stage1 = xor(reply, sha1(public_seed, hash_stage2))
 *          candidate_hash2 = sha1(hash_stage1)
 *          check(candidate_hash2 == hash_stage2)
 * </pre>
 */
public class MysqlPassword {

    private static final Logger LOG = LogManager.getLogger(MysqlPassword.class);

    /**
     * Empty password constant
     */
    public static final byte[] EMPTY_PASSWORD = new byte[0];

    /**
     * Length of the scramble string
     */
    public static final int SCRAMBLE_LENGTH = 20;

    /**
     * Length of the hex-encoded scramble string (with leading *)
     */
    public static final int SCRAMBLE_LENGTH_HEX_LENGTH = 2 * SCRAMBLE_LENGTH + 1;

    /**
     * Protocol version 4.1 password prefix character
     */
    public static final byte PVERSION41_CHAR = '*';

    public static final long VALIDATE_PASSWORD_POLICY_DISABLED = 0;
    public static final long VALIDATE_PASSWORD_POLICY_STRONG = 2;
    public static final int MIN_PASSWORD_LEN = 8;

    private static final byte[] DIG_VEC_UPPER = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    private static final String SPECIAL_CHARS = "~!@#$%^&*()_+|<>,.?/:;'[]{}";
    private static final Set<Character> COMPLEX_CHAR_SET;

    /**
     * Built-in dictionary of common weak password words.
     * Used as fallback when no external dictionary file is configured.
     * Password containing any of these words (case-insensitive) will be rejected under STRONG policy.
     */
    private static final Set<String> BUILTIN_DICTIONARY_WORDS;

    // Lazy-loaded dictionary from external file
    private static volatile Set<String> loadedDictionaryWords = null;
    // The file path that was used to load the dictionary (for detecting changes)
    private static volatile String loadedDictionaryFilePath = null;
    // Lock object for thread-safe lazy loading
    private static final Object DICTIONARY_LOAD_LOCK = new Object();

    private static final Random RANDOM = new SecureRandom();

    static {
        COMPLEX_CHAR_SET = new HashSet<>();
        for (char c : SPECIAL_CHARS.toCharArray()) {
            COMPLEX_CHAR_SET.add(c);
        }

        BUILTIN_DICTIONARY_WORDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            // Common password words
            "password", "passwd", "pass", "pwd", "secret",
            // User/role related
            "admin", "administrator", "root", "user", "guest", "login", "master", "super",
            // Test/demo related
            "test", "testing", "demo", "sample", "example", "temp", "temporary",
            // System/database related
            "system", "server", "database", "mysql", "doris", "oracle", "postgres",
            // Common weak patterns
            "qwerty", "abc", "letmein", "welcome", "hello", "monkey", "dragon", "iloveyou",
            "trustno", "sunshine", "princess", "football", "baseball", "soccer"
        )));
    }

    private MysqlPassword() {
        // Utility class
    }

    /**
     * Get the dictionary words to use for password validation.
     * If an external dictionary file is configured, load it lazily.
     * Otherwise, use the built-in dictionary.
     *
     * @return the set of dictionary words (all in lowercase)
     */
    private static Set<String> getDictionaryWords() {
        String configuredFileName = getConfiguredDictionaryFileName();

        // If no file is configured, use built-in dictionary
        if (configuredFileName == null || configuredFileName.isEmpty()) {
            return BUILTIN_DICTIONARY_WORDS;
        }

        String baseDir = Config.security_plugins_dir;
        String configuredFilePath = baseDir == null
                    ? configuredFileName
                    : Paths.get(baseDir, configuredFileName).normalize().toString();
        // Check if we need to (re)load the dictionary
        // Double-checked locking for thread safety
        if (loadedDictionaryWords == null || !configuredFilePath.equals(loadedDictionaryFilePath)) {
            synchronized (DICTIONARY_LOAD_LOCK) {
                if (loadedDictionaryWords == null || !configuredFilePath.equals(loadedDictionaryFilePath)) {
                    loadedDictionaryWords = loadDictionaryFromFile(configuredFilePath);
                    loadedDictionaryFilePath = configuredFilePath;
                }
            }
        }

        return loadedDictionaryWords != null ? loadedDictionaryWords : BUILTIN_DICTIONARY_WORDS;
    }

    private static String getConfiguredDictionaryFileName() {
        try {
            Class<?> clazz = Class.forName("org.apache.doris.qe.GlobalVariable");
            Object value = clazz.getDeclaredField("validatePasswordDictionaryFile").get(null);
            if (value instanceof String) {
                return (String) value;
            }
        } catch (ClassNotFoundException e) {
            // GlobalVariable not available in this module.
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOG.warn("Failed to read GlobalVariable.validatePasswordDictionaryFile: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Load dictionary words from an external file.
     * Each line in the file is treated as one dictionary word.
     * Empty lines and lines starting with '#' are ignored.
     *
     * @param filePath path to the dictionary file
     * @return set of dictionary words (all converted to lowercase), or null if loading fails
     */
    private static Set<String> loadDictionaryFromFile(String filePath) {
        Set<String> words = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Skip empty lines and comments
                if (!line.isEmpty() && !line.startsWith("#")) {
                    words.add(line.toLowerCase());
                }
            }
            LOG.info("Loaded {} words from password dictionary file: {}", words.size(), filePath);
            return words;
        } catch (IOException e) {
            LOG.warn("Failed to load password dictionary file: {}. Using built-in dictionary. Error: {}",
                    filePath, e.getMessage());
            return null;
        }
    }

    /**
     * Creates a random string for the authentication challenge.
     *
     * @param len length of the random string
     * @return random byte array
     */
    public static byte[] createRandomString(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        // NOTE: MySQL challenge string can't contain 0.
        for (int i = 0; i < len; ++i) {
            if (!((bytes[i] >= 'a' && bytes[i] <= 'z') || (bytes[i] >= 'A' && bytes[i] <= 'Z'))) {
                bytes[i] = (byte) ('a' + (bytes[i] % 26));
            }
        }
        return bytes;
    }

    /**
     * XOR two byte arrays.
     */
    private static byte[] xorCrypt(byte[] s1, byte[] s2) {
        if (s1.length != s2.length) {
            return null;
        }
        byte[] res = new byte[s1.length];
        for (int i = 0; i < s1.length; ++i) {
            res[i] = (byte) (s1[i] ^ s2[i]);
        }
        return res;
    }

    /**
     * Verifies that a scrambled password matches the stored hash.
     *
     * @param scramble   the scrambled password from client
     * @param message    the random challenge string sent to client
     * @param hashStage2 the stored password hash (two-stage hash)
     * @return true if password is correct
     */
    public static boolean checkScramble(byte[] scramble, byte[] message, byte[] hashStage2) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("No SHA-1 Algorithm when compute password.");
            return false;
        }
        // compute result1: XOR(scramble, SHA-1(public_seed + hashStage2))
        md.update(message);
        md.update(hashStage2);
        byte[] hashStage1 = xorCrypt(md.digest(), scramble);

        // compute result2: SHA-1(result1)
        md.reset();
        md.update(hashStage1);
        byte[] candidateHash2 = md.digest();

        // compare result2 and hashStage2
        return MessageDigest.isEqual(candidateHash2, hashStage2);
    }

    /**
     * Creates a scrambled password from the seed and plain text password.
     * This is what the MySQL client sends to the server.
     *
     * @param seed     the random challenge from server
     * @param password the plain text password
     * @return scrambled password bytes
     */
    public static byte[] scramble(byte[] seed, String password) {
        byte[] scramblePassword = null;
        try {
            byte[] passBytes = password.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hashStage1 = md.digest(passBytes);
            md.reset();
            byte[] hashStage2 = md.digest(hashStage1);
            md.reset();
            md.update(seed);
            scramblePassword = xorCrypt(hashStage1, md.digest(hashStage2));
        } catch (UnsupportedEncodingException e) {
            LOG.warn("No UTF-8 character set when compute password.");
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("No SHA-1 Algorithm when compute password.");
        }
        return scramblePassword;
    }

    /**
     * Creates the two-stage hash of a password for storage.
     *
     * @param password plain text password
     * @return two-stage SHA-1 hash
     */
    private static byte[] twoStageHash(String password) {
        try {
            byte[] passBytes = password.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hashStage1 = md.digest(passBytes);
            md.reset();
            return md.digest(hashStage1);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("No UTF-8 character set when compute password.");
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("No SHA-1 Algorithm when compute password.");
        }
        return null;
    }

    /**
     * Converts octet bytes to hexadecimal representation.
     */
    private static void octetToHexSafe(byte[] to, int toOff, byte[] from) {
        int j = toOff;
        for (byte b : from) {
            int val = b & 0xff;
            to[j++] = DIG_VEC_UPPER[val >> 4];
            to[j++] = DIG_VEC_UPPER[val & 0x0f];
        }
    }

    private static int fromByte(int b) {
        return (b >= '0' && b <= '9') ? b - '0'
            : (b >= 'A' && b <= 'F') ? b - 'A' + 10
            : b - 'a' + 10;
    }

    /**
     * Converts hex string to octet bytes.
     */
    private static void hexToOctetSafe(byte[] to, byte[] from, int fromOff) {
        int j = 0;
        for (int i = fromOff; i < from.length; i++) {
            int val = fromByte(from[i++] & 0xff);
            to[j++] = ((byte) ((val << 4) + fromByte(from[i] & 0xff)));
        }
    }

    /**
     * Creates a scrambled password string for storage from plain text.
     *
     * <p>The format is: *[40 hex characters representing SHA1(SHA1(password))]
     *
     * @param plainPasswd plain text password
     * @return scrambled password bytes (41 bytes starting with '*')
     */
    public static byte[] makeScrambledPassword(String plainPasswd) {
        if (plainPasswd == null || plainPasswd.isEmpty()) {
            return EMPTY_PASSWORD;
        }

        byte[] hashStage2 = twoStageHash(plainPasswd);
        byte[] passwd = new byte[SCRAMBLE_LENGTH_HEX_LENGTH];
        passwd[0] = PVERSION41_CHAR;
        octetToHexSafe(passwd, 1, hashStage2);
        return passwd;
    }

    /**
     * Extracts the binary hash from a stored scrambled password.
     *
     * @param password stored password (hex format starting with '*')
     * @return binary hash (20 bytes)
     */
    public static byte[] getSaltFromPassword(byte[] password) {
        if (password == null || password.length == 0) {
            return EMPTY_PASSWORD;
        }
        byte[] hashStage2 = new byte[SCRAMBLE_LENGTH];
        hexToOctetSafe(hashStage2, password, 1);
        return hashStage2;
    }

    /**
     * Checks if a plain password matches a scrambled password.
     *
     * @param scrambledPass stored scrambled password
     * @param plainPass     plain text password to check
     * @return true if passwords match
     */
    public static boolean checkPlainPass(byte[] scrambledPass, String plainPass) {
        byte[] pass = makeScrambledPassword(plainPass);
        if (pass.length != scrambledPass.length) {
            return false;
        }
        for (int i = 0; i < pass.length; ++i) {
            if (pass[i] != scrambledPass[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates a plain text password according to the specified policy.
     *
     * <p>Policy values:
     * <ul>
     *   <li>0: Disabled - no validation</li>
     *   <li>2: STRONG - password must be at least 8 characters, contain all 4 types
     *       of: numbers, uppercase letters, lowercase letters, and special characters,
     *       and must not contain dictionary words</li>
     * </ul>
     *
     * @param policy   the password validation policy (0 = disabled, 2 = STRONG)
     * @param password the plain text password to validate
     * @throws IllegalArgumentException if password does not meet policy requirements
     */
    public static void validatePlainPassword(long policy, String password) {
        if (policy == VALIDATE_PASSWORD_POLICY_DISABLED) {
            // Policy disabled, no validation
            return;
        }

        if (policy == VALIDATE_PASSWORD_POLICY_STRONG) {
            // STRONG policy
            if (password == null || password.length() < MIN_PASSWORD_LEN) {
                throw new IllegalArgumentException(
                    "Violate password validation policy: STRONG. "
                        + "The password must be at least " + MIN_PASSWORD_LEN + " characters.");
            }

            StringBuilder missingTypes = new StringBuilder();

            if (password.chars().noneMatch(Character::isDigit)) {
                missingTypes.append("numeric, ");
            }
            if (password.chars().noneMatch(Character::isLowerCase)) {
                missingTypes.append("lowercase, ");
            }
            if (password.chars().noneMatch(Character::isUpperCase)) {
                missingTypes.append("uppercase, ");
            }
            if (password.chars().noneMatch(c -> COMPLEX_CHAR_SET.contains((char) c))) {
                missingTypes.append("special character, ");
            }

            if (missingTypes.length() > 0) {
                // Remove trailing ", "
                missingTypes.setLength(missingTypes.length() - 2);
                throw new IllegalArgumentException(
                    "Violate password validation policy: STRONG. "
                        + "The password must contain at least one character from each of the following types: "
                        + "numeric, lowercase, uppercase, and special characters. "
                        + "Missing: " + missingTypes + ".");
            }

            // Check for dictionary words (case-insensitive)
            String foundWord = containsDictionaryWord(password);
            if (foundWord != null) {
                throw new IllegalArgumentException(
                    "Violate password validation policy: STRONG. "
                        + "The password contains a common dictionary word '" + foundWord + "', "
                        + "which makes it easy to guess. Please choose a more secure password.");
            }
        } else {
            throw new IllegalArgumentException("Unknown password validation policy: " + policy);
        }
    }

    /**
     * Check if the password contains any dictionary word (case-insensitive).
     * Uses either the external dictionary file (if configured) or the built-in dictionary.
     *
     * @param password the password to check
     * @return the found dictionary word, or null if none found
     */
    private static String containsDictionaryWord(String password) {
        String lowerPassword = password.toLowerCase();
        for (String word : getDictionaryWords()) {
            if (lowerPassword.contains(word)) {
                return word;
            }
        }
        return null;
    }

    /**
     * Validates and converts a scrambled password string to bytes.
     *
     * <p>The password string should be in the format: *[40 hex characters]
     * representing SHA1(SHA1(password)).
     *
     * @param password scrambled password string (null or format: *[40 hex chars])
     * @return password bytes (empty if input is null)
     * @throws IllegalArgumentException if password format is invalid
     */
    public static byte[] checkPassword(String password) {
        if (password == null || password.isEmpty()) {
            return EMPTY_PASSWORD;
        }

        // Check length: should be 41 characters (* + 40 hex chars)
        if (password.length() != SCRAMBLE_LENGTH_HEX_LENGTH) {
            throw new IllegalArgumentException(
                "Password length should be " + SCRAMBLE_LENGTH_HEX_LENGTH + " characters");
        }

        // Check first character should be '*'
        if (password.charAt(0) != PVERSION41_CHAR) {
            throw new IllegalArgumentException(
                "Password should start with '*' character");
        }

        // Check remaining characters should be hex digits
        byte[] passwordBytes = password.getBytes();
        for (int i = 1; i < passwordBytes.length; i++) {
            byte b = passwordBytes[i];
            if (!((b >= '0' && b <= '9') || (b >= 'A' && b <= 'F') || (b >= 'a' && b <= 'f'))) {
                throw new IllegalArgumentException(
                    "Password contains invalid hex character at position " + i);
            }
        }

        // Convert hex string to bytes
        return passwordBytes;
    }
}


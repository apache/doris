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

package org.apache.doris.protocol.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

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
 * 
 * @since 2.0.0
 */
public class MysqlPassword {
    
    private static final Logger LOG = LogManager.getLogger(MysqlPassword.class);
    
    /** Empty password constant */
    public static final byte[] EMPTY_PASSWORD = new byte[0];
    
    /** Length of the scramble string */
    public static final int SCRAMBLE_LENGTH = 20;
    
    /** Length of the hex-encoded scramble string (with leading *) */
    public static final int SCRAMBLE_LENGTH_HEX_LENGTH = 2 * SCRAMBLE_LENGTH + 1;
    
    /** Protocol version 4.1 password prefix character */
    public static final byte PVERSION41_CHAR = '*';
    
    private static final byte[] DIG_VEC_UPPER = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };
    
    private static final Random RANDOM = new SecureRandom();

    private MysqlPassword() {
        // Utility class
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
            if (!((bytes[i] >= 'a' && bytes[i] <= 'z')
                    || (bytes[i] >= 'A' && bytes[i] <= 'Z'))) {
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
     * @param scramble the scrambled password from client
     * @param message the random challenge string sent to client
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
     * @param seed the random challenge from server
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
     * @param plainPass plain text password to check
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
}

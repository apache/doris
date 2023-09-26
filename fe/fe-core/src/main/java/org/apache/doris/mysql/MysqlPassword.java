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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

// this is stolen from MySQL
//
// The main idea is that no password are sent between client & server on
// connection and that no password are saved in mysql in a decodable form.
//
// On connection a random string is generated and sent to the client.
// The client generates a new string with a random generator inited with
// the hash values from the password and the sent string.
// This 'check' string is sent to the server where it is compared with
// a string generated from the stored hash_value of the password and the
// random string.
//
// The password is saved (in user.password) by using the PASSWORD() function in
// mysql.
//
// This is .c file because it's used in libmysqlclient, which is entirely in C.
// (we need it to be portable to a variety of systems).
// Example:
// update user set password=PASSWORD("hello") where user="test"
// This saves a hashed number as a string in the password field.
//
// The new authentication is performed in following manner:
//
// SERVER:  public_seed=create_random_string()
//          send(public_seed)
//
// CLIENT:  recv(public_seed)
//          hash_stage1=sha1("password")
//          hash_stage2=sha1(hash_stage1)
//          reply=xor(hash_stage1, sha1(public_seed,hash_stage2)
//
//          this three steps are done in scramble()
//
//          send(reply)
//
// SERVER:  recv(reply)
//          hash_stage1=xor(reply, sha1(public_seed,hash_stage2))
//          candidate_hash2=sha1(hash_stage1)
//          check(candidate_hash2==hash_stage2)
//
//          this three steps are done in check_scramble()
public class MysqlPassword {
    private static final Logger LOG = LogManager.getLogger(MysqlPassword.class);
    // TODO(zhaochun): this is duplicated with handshake packet.
    public static final byte[] EMPTY_PASSWORD = new byte[0];
    public static final int SCRAMBLE_LENGTH = 20;
    public static final int SCRAMBLE_LENGTH_HEX_LENGTH = 2 * SCRAMBLE_LENGTH + 1;
    public static final byte PVERSION41_CHAR = '*';
    private static final byte[] DIG_VEC_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static final Random random = new SecureRandom();
    private static final Set<Character> complexCharSet;
    public static final int MIN_PASSWORD_LEN = 8;

    static {
        complexCharSet = "~!@#$%^&*()_+|<>,.?/:;'[]{}".chars().mapToObj(c -> (char) c).collect(Collectors.toSet());
    }

    public static byte[] createRandomString(int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        // NOTE: MySQL challenge string can't contain 0.
        for (int i = 0; i < len; ++i) {
            if (!((bytes[i] >= 'a' && bytes[i] <= 'z')
                    || (bytes[i] >= 'A' && bytes[i] <= 'Z'))) {
                bytes[i] = (byte) ('a' + (bytes[i] % 26));
            }
        }
        return bytes;
    }

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

    // Check that scrambled message corresponds to the password; the function
    // is used by server to check that received reply is authentic.
    // This function does not check lengths of given strings: message must be
    // null-terminated, reply and hash_stage2 must be at least SHA1_HASH_SIZE
    // long (if not, something fishy is going on).
    // SYNOPSIS
    //   check_scramble_sha1()
    //   scramble     clients' reply, presumably produced by scramble()
    //   message      original random string, previously sent to client
    //                (presumably second argument of scramble()), must be
    //                exactly SCRAMBLE_LENGTH long and NULL-terminated.
    //   hash_stage2  hex2octet-decoded database entry
    //   All params are IN.
    //
    // RETURN VALUE
    //   0  password is correct
    //   !0  password is invalid
    public static boolean checkScramble(byte[] scramble, byte[] message, byte[] hashStage2) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("No SHA-1 Algorithm when compute password.");
            return false;
        }
        // compute result1: XOR(scramble, SHA-1 (public_seed + hashStage2))
        md.update(message);
        md.update(hashStage2);
        byte[] hashStage1 = xorCrypt(md.digest(), scramble);

        // compute result2: SHA-1(result1)
        md.reset();
        md.update(hashStage1);
        byte[] candidateHash2 = md.digest();
        // compare result2 and hashStage2 using MessageDigest.isEqual()
        return MessageDigest.isEqual(candidateHash2, hashStage2);
    }

    // MySQL client use this function to form scramble password
    // password: plaintext password
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
            // no UTF-8 character set
            LOG.warn("No UTF-8 character set when compute password.");
        } catch (NoSuchAlgorithmException e) {
            // No SHA-1 algorithm
            LOG.warn("No SHA-1 Algorithm when compute password.");
        }

        return scramblePassword;
    }

    // 将用户传入的字符串转化为对应的密码Hash值
    // 用于用户设定密码
    private static byte[] twoStageHash(String password) {
        try {
            byte[] passBytes = password.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] hashStage1 = md.digest(passBytes);
            md.reset();
            byte[] hashStage2 = md.digest(hashStage1);

            return hashStage2;
        } catch (UnsupportedEncodingException e) {
            // no UTF-8 character set
            LOG.warn("No UTF-8 character set when compute password.");
        } catch (NoSuchAlgorithmException e) {
            // No SHA-1 algorithm
            LOG.warn("No SHA-1 Algorithm when compute password.");
        }

        return null;
    }

    // covert octet 'from' to hex 'to'
    // NOTE: this function assume that to buffer is enough
    private static void octetToHexSafe(byte[] to, int toOff, byte[] from) {
        int j = toOff;
        for (int i = 0; i < from.length; i++) {
            int val = from[i] & 0xff;
            to[j++] = DIG_VEC_UPPER[val >> 4];
            to[j++] = DIG_VEC_UPPER[val & 0x0f];
        }
    }

    private static int fromByte(int b) {
        return (b >= '0' && b <= '9') ? b - '0'
                : (b >= 'A' && b <= 'F') ? b - 'A' + 10 : b - 'a' + 10;
    }

    // covert hex 'from' to octet 'to'
    // fromOff: offset of 'from' to covert, there is no pointer in JAVA
    // NOTE: this function assume that to buffer is enough
    private static void hexToOctetSafe(byte[] to, byte[] from, int fromOff) {
        int j = 0;
        for (int i = fromOff; i < from.length; i++) {
            int val = fromByte(from[i++] & 0xff);
            to[j++] = ((byte) ((val << 4) + fromByte(from[i] & 0xff)));
        }
    }

    // Make password which stored in palo meta from plain text
    public static byte[] makeScrambledPassword(String plainPasswd) {
        if (Strings.isNullOrEmpty(plainPasswd)) {
            return EMPTY_PASSWORD;
        }

        byte[] hashStage2 = twoStageHash(plainPasswd);
        byte[] passwd = new byte[SCRAMBLE_LENGTH_HEX_LENGTH];
        passwd[0] = (PVERSION41_CHAR);
        octetToHexSafe(passwd, 1, hashStage2);
        return passwd;
    }

    // Convert scrambled password from ascii hex string to binary form.
    public static byte[] getSaltFromPassword(byte[] password) {
        if (password == null || password.length == 0) {
            return EMPTY_PASSWORD;
        }
        byte[] hashStage2 = new byte[SCRAMBLE_LENGTH];
        hexToOctetSafe(hashStage2, password, 1);
        return hashStage2;
    }

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

    public static byte[] checkPassword(String passwdString) throws AnalysisException {
        if (Strings.isNullOrEmpty(passwdString)) {
            return EMPTY_PASSWORD;
        }

        byte[] passwd = null;
        try {
            passwdString = passwdString.toUpperCase();
            passwd = passwdString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_ERROR);
        }
        if (passwd.length != SCRAMBLE_LENGTH_HEX_LENGTH || passwd[0] != PVERSION41_CHAR) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_PASSWD_LENGTH, 41);
        }

        for (int i = 1; i < passwd.length; ++i) {
            if (!((passwd[i] <= '9' && passwd[i] >= '0') || passwd[i] >= 'A' && passwd[i] <= 'F')) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_PASSWD_LENGTH, 41);
            }
        }

        return passwd;
    }

    public static void validatePlainPassword(long validaPolicy, String text) throws AnalysisException {
        if (validaPolicy == GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG) {
            if (Strings.isNullOrEmpty(text) || text.length() < MIN_PASSWORD_LEN) {
                throw new AnalysisException(
                        "Violate password validation policy: STRONG. The password must be at least 8 characters");
            }

            int i = 0;
            if (text.chars().anyMatch(Character::isDigit)) {
                i++;
            }
            if (text.chars().anyMatch(Character::isLowerCase)) {
                i++;
            }
            if (text.chars().anyMatch(Character::isUpperCase)) {
                i++;
            }
            if (text.chars().anyMatch(c -> complexCharSet.contains((char) c))) {
                i++;
            }
            if (i < 3) {
                throw new AnalysisException(
                        "Violate password validation policy: STRONG. The password must contain at least 3 types of "
                                + "numbers, uppercase letters, lowercase letters and special characters.");
            }
        }
    }
}

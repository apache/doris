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

package org.apache.doris.stack.util;

import org.mindrot.jbcrypt.BCrypt;
import org.bouncycastle.crypto.digests.SHA3Digest;

import java.util.Random;

/**
 * @Description：A tool class that handles one-way encryption and authentication of passwords
 */
public class CredsUtil {

    private CredsUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * One way encryption password
     *
     * @param password  password
     * @param logRounds logRounds
     * @return string
     */
    public static String hashBcrypt(String password, int logRounds) {
        if (logRounds <= 0) {
            return BCrypt.hashpw(password, BCrypt.gensalt());
        } else {
            return BCrypt.hashpw(password, BCrypt.gensalt(logRounds));
        }
    }

    /**
     * Verify that the password is correct
     *
     * @param password password
     * @param hash     hash
     * @return boolean
     */
    public static boolean bcryptVerify(String password, String hash) {
        return BCrypt.checkpw(password, hash);
    }

    /**
     * Randomly generate a password of the specified length
     *
     * @param random random
     * @param len    length
     * @return string
     */
    public static String createPassWord(int random, int len) {
        Random rd = new Random(random);
        int maxNum = 62;
        StringBuffer sb = new StringBuffer();
        int rdGet; // Get random number
        char[] str = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
                'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
                'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5',
                '6', '7', '8', '9'};
        int count = 0;
        while (count < len) {
            rdGet = Math.abs(rd.nextInt(maxNum)); // The maximum number of generated is 62-1
            if (rdGet >= 0 && rdGet < str.length) {
                sb.append(str[rdGet]);
                count++;
            }
        }
        return sb.toString();
    }

    /**
     * Gets the hash value of the string
     *
     * @param context context
     * @return hash string
     */
    public static String hashSha3256(String context) {
        SHA3Digest digest = new SHA3Digest(256);
        byte[] data = context.getBytes();
        digest.update(data, 0, data.length);

        byte[] result = new byte[digest.getDigestSize()];
        digest.doFinal(result, 0);
        return org.bouncycastle.util.encoders.Hex.toHexString(result);
    }
}

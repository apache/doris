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

package org.apache.doris.common;

import java.security.SecureRandom;

public class RandomIdentifierGenerator {

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String ALPHANUMERIC = ALPHABET + "0123456789_";
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Generates a random identifier matching the pattern "^[a-zA-Z][a-zA-Z0-9_]*$".
     *
     * @param length The desired length of the identifier (must be at least 1)
     * @return A randomly generated identifier
     */
    public static String generateRandomIdentifier(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("Length must be at least 1");
        }

        StringBuilder sb = new StringBuilder(length);

        // First character must be a letter
        sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));

        // Remaining characters can be alphanumeric or underscore
        for (int i = 1; i < length; i++) {
            sb.append(ALPHANUMERIC.charAt(RANDOM.nextInt(ALPHANUMERIC.length())));
        }

        return sb.toString();
    }
}

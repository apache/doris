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

package org.apache.doris.authentication.plugin.ldap;

/**
 * Test utility class for Java 8 compatibility.
 *
 * <p>Provides helper methods that are available in Java 11+ but missing in Java 8.
 */
public final class TestUtils {

    private TestUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Repeats the given character n times (Java 8 compatible).
     *
     * <p>Equivalent to String.repeat() in Java 11+.
     *
     * @param ch the character to repeat
     * @param count the number of times to repeat
     * @return string with repeated character
     */
    public static String repeat(char ch, int count) {
        if (count <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     * Repeats the given string n times (Java 8 compatible).
     *
     * <p>Equivalent to String.repeat() in Java 11+.
     *
     * @param str the string to repeat
     * @param count the number of times to repeat
     * @return repeated string
     */
    public static String repeat(String str, int count) {
        if (str == null || str.isEmpty() || count <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(str.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}

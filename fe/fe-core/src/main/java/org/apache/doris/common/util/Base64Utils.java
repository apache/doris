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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

public class Base64Utils {

    /**
     * Encodes an integer part ID into a Base64 string using a fixed 4-byte
     * Big-Endian representation.
     * <p>
     * This ensures consistent encoding across different platforms
     * (Java, C++, JavaScript) regardless of machine endianness.
     * Unlike a regular Base64 encoding that operates on text strings,
     * this method encodes the raw binary form of the integer value.
     * <p>
     * For example:
     * <ul>
     *   <li>id = 1 → bytes [0x00, 0x00, 0x00, 0x01]</li>
     *   <li>Base64 result = "AAAAAQ=="</li>
     * </ul>
     * This matches the same logic as C++ implementations using Big-Endian
     * byte order and Base64 encoding.
     */
    public static String encodeToBase64(int id) {
        ByteBuffer buf = ByteBuffer.allocate(4)
                .order(ByteOrder.BIG_ENDIAN);
        buf.putInt(id);
        return Base64.getEncoder().encodeToString(buf.array());
    }
}

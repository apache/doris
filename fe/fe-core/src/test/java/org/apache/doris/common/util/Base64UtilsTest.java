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


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

public class Base64UtilsTest {
    @Test
    public void testEncodeToBase64_basicCases() {
        // 1 → [00 00 00 01] → AAAAAQ==
        Assertions.assertEquals("AAAAAQ==", Base64Utils.encodeToBase64(1));

        // 255 → [00 00 00 FF] → AAAA/w==
        Assertions.assertEquals("AAAA/w==", Base64Utils.encodeToBase64(255));

        // 256 → [00 00 01 00] → AAAAIA==
        Assertions.assertEquals("AAABAA==", Base64Utils.encodeToBase64(256));

        // 0 → [00 00 00 00] → AAAAAA==
        Assertions.assertEquals("AAAAAA==", Base64Utils.encodeToBase64(0));

        // -1 → [FF FF FF FF] → /////w==
        Assertions.assertEquals("/////w==", Base64Utils.encodeToBase64(-1));

        // Integer.MAX_VALUE → [7F FF FF FF] → f////w==
        Assertions.assertEquals("f////w==", Base64Utils.encodeToBase64(Integer.MAX_VALUE));

        // Integer.MIN_VALUE → [80 00 00 00] → gAAA==
        Assertions.assertEquals("gAAAAA==", Base64Utils.encodeToBase64(Integer.MIN_VALUE));
    }

    @Test
    public void testEncodeToBase64_matchesManualEncoding() {
        int id = 12345;

        // Manually encode the integer using Big-Endian order
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(id);
        String expected = Base64.getEncoder().encodeToString(buf.array());

        // The result from Base64Utils must match
        String actual = Base64Utils.encodeToBase64(id);

        Assertions.assertEquals(expected, actual);
    }
}

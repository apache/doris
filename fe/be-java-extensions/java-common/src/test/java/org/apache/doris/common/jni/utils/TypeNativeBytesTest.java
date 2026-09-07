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

package org.apache.doris.common.jni.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class TypeNativeBytesTest {
    @Test
    public void testUuidNativeByteOrderRoundTrip() {
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        byte[] expected = new byte[] {
                (byte) 0xff, (byte) 0xee, (byte) 0xdd, (byte) 0xcc,
                (byte) 0xbb, (byte) 0xaa, (byte) 0x99, (byte) 0x88,
                (byte) 0x77, (byte) 0x66, (byte) 0x55, (byte) 0x44,
                (byte) 0x33, (byte) 0x22, (byte) 0x11, (byte) 0x00
        };

        Assert.assertArrayEquals(expected, TypeNativeBytes.getUuidBytes(uuid));
        Assert.assertEquals(uuid, TypeNativeBytes.getUuid(expected));
    }
}

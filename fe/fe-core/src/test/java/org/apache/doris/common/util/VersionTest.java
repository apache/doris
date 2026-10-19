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

public class VersionTest {

    @Test
    public void testVersion() {
        DigitalVersion v = new DigitalVersion(1100000);

        System.out.println(v);
        Assertions.assertEquals(1, v.major);
        Assertions.assertEquals(10, v.minor);
        Assertions.assertEquals(0, v.revision);

        DigitalVersion s = new DigitalVersion((byte) 50, (byte) 2, (byte) 3);
        Assertions.assertEquals(50020300, s.id);

        Assertions.assertTrue(s.onOrAfter(v));
        Assertions.assertFalse(s.before(v));

        DigitalVersion vs = new DigitalVersion((byte) 1, (byte) 10, (byte) 0);
        Assertions.assertEquals(vs, v);
    }

    @Test
    public void testFromString() {
        try {
            Assertions.assertEquals(1060000, DigitalVersion.fromString("1.6.0.123.123").id);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        try {
            DigitalVersion.fromString("");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("Illegal empty version"));
        }

        try {
            DigitalVersion.fromString("1.6123123.123");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("Illegal version format"));
        }

        try {
            DigitalVersion.fromString("a.b.c");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("Illegal version format"));
        }
    }
}

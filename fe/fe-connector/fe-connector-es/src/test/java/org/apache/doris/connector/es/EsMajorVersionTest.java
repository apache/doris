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

package org.apache.doris.connector.es;

import org.apache.doris.connector.api.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EsMajorVersionTest {

    // === parse ===

    @Test
    void testParseVersion7() {
        EsMajorVersion v = EsMajorVersion.parse("7.10.2");
        Assertions.assertEquals(7, v.major);
        Assertions.assertEquals("7.10.2", v.toString());
    }

    @Test
    void testParseVersion8() {
        EsMajorVersion v = EsMajorVersion.parse("8.0.0");
        Assertions.assertEquals(8, v.major);
    }

    @Test
    void testParseVersion6() {
        EsMajorVersion v = EsMajorVersion.parse("6.8.23");
        Assertions.assertEquals(6, v.major);
    }

    @Test
    void testParseVersion5() {
        EsMajorVersion v = EsMajorVersion.parse("5.6.16");
        Assertions.assertEquals(5, v.major);
    }

    @Test
    void testParseOldVersions() {
        Assertions.assertEquals(0, EsMajorVersion.parse("0.90.0").major);
        Assertions.assertEquals(1, EsMajorVersion.parse("1.7.6").major);
        Assertions.assertEquals(2, EsMajorVersion.parse("2.4.6").major);
    }

    @Test
    void testParseUnsupportedVersionThrows() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> EsMajorVersion.parse("9.0.0"));
        Assertions.assertThrows(DorisConnectorException.class,
                () -> EsMajorVersion.parse("3.0.0"));
    }

    // === comparison methods ===

    @Test
    void testAfter() {
        Assertions.assertTrue(EsMajorVersion.V_8_X.after(EsMajorVersion.V_7_X));
        Assertions.assertFalse(EsMajorVersion.V_7_X.after(EsMajorVersion.V_8_X));
        Assertions.assertFalse(EsMajorVersion.V_7_X.after(EsMajorVersion.V_7_X));
    }

    @Test
    void testOn() {
        Assertions.assertTrue(EsMajorVersion.V_7_X.on(EsMajorVersion.V_7_X));
        Assertions.assertFalse(EsMajorVersion.V_7_X.on(EsMajorVersion.V_8_X));
    }

    @Test
    void testNotOn() {
        Assertions.assertTrue(EsMajorVersion.V_7_X.notOn(EsMajorVersion.V_8_X));
        Assertions.assertFalse(EsMajorVersion.V_7_X.notOn(EsMajorVersion.V_7_X));
    }

    @Test
    void testOnOrAfter() {
        Assertions.assertTrue(EsMajorVersion.V_8_X.onOrAfter(EsMajorVersion.V_7_X));
        Assertions.assertTrue(EsMajorVersion.V_7_X.onOrAfter(EsMajorVersion.V_7_X));
        Assertions.assertFalse(EsMajorVersion.V_6_X.onOrAfter(EsMajorVersion.V_7_X));
    }

    @Test
    void testBefore() {
        Assertions.assertTrue(EsMajorVersion.V_6_X.before(EsMajorVersion.V_7_X));
        Assertions.assertFalse(EsMajorVersion.V_7_X.before(EsMajorVersion.V_7_X));
    }

    @Test
    void testOnOrBefore() {
        Assertions.assertTrue(EsMajorVersion.V_6_X.onOrBefore(EsMajorVersion.V_7_X));
        Assertions.assertTrue(EsMajorVersion.V_7_X.onOrBefore(EsMajorVersion.V_7_X));
        Assertions.assertFalse(EsMajorVersion.V_8_X.onOrBefore(EsMajorVersion.V_7_X));
    }

    // === equals / hashCode ===

    @Test
    void testEqualsSameMajor() {
        EsMajorVersion a = EsMajorVersion.parse("7.10.2");
        EsMajorVersion b = EsMajorVersion.parse("7.1.0");
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testNotEqualsDifferentMajor() {
        Assertions.assertNotEquals(EsMajorVersion.V_7_X, EsMajorVersion.V_8_X);
    }

    @Test
    void testLatestIsV8() {
        Assertions.assertEquals(EsMajorVersion.V_8_X, EsMajorVersion.LATEST);
    }
}

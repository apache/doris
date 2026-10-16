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

package org.apache.doris.kerberos;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KerberosAuthSpecTest {

    @Test
    void accessorsExposePrincipalAndKeytab() {
        KerberosAuthSpec spec = new KerberosAuthSpec("hive/_HOST@REALM", "/etc/security/hive.keytab");

        Assertions.assertEquals("hive/_HOST@REALM", spec.getPrincipal());
        Assertions.assertEquals("/etc/security/hive.keytab", spec.getKeytab());
    }

    @Test
    void hasCredentials_requiresBothPrincipalAndKeytab() {
        // Intent: a usable doAs login needs BOTH a principal and a keytab; either missing
        // (null or blank) means there is no static Kerberos login to perform.
        Assertions.assertTrue(new KerberosAuthSpec("p", "k").hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec("p", "").hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec("p", null).hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec("", "k").hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec(null, "k").hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec("  ", "  ").hasCredentials());
        Assertions.assertFalse(new KerberosAuthSpec(null, null).hasCredentials());
    }

    @Test
    void valueSemantics_equalsAndHashCode() {
        KerberosAuthSpec a = new KerberosAuthSpec("p", "k");
        KerberosAuthSpec b = new KerberosAuthSpec("p", "k");
        KerberosAuthSpec c = new KerberosAuthSpec("p", "other");

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
    }
}

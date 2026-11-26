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

class AuthTypeTest {

    @Test
    void fromString_resolvesKerberosOnlyForExplicitKerberos() {
        // Intent: a connection is treated as Kerberos-secured ONLY when the auth type is
        // explicitly "kerberos" (case/whitespace-insensitive); everything else is SIMPLE.
        Assertions.assertEquals(AuthType.KERBEROS, AuthType.fromString("kerberos"));
        Assertions.assertEquals(AuthType.KERBEROS, AuthType.fromString("KERBEROS"));
        Assertions.assertEquals(AuthType.KERBEROS, AuthType.fromString("  Kerberos  "));
    }

    @Test
    void fromString_resolvesEverythingElseToSimple() {
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString(null));
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString(""));
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString("   "));
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString("simple"));
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString("none"));
        Assertions.assertEquals(AuthType.SIMPLE, AuthType.fromString("anything"));
    }

    @Test
    void getDesc_returnsLowercaseWireName() {
        Assertions.assertEquals("simple", AuthType.SIMPLE.getDesc());
        Assertions.assertEquals("kerberos", AuthType.KERBEROS.getDesc());
    }
}

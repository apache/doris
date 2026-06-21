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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * Pins the JDK-only KerberosTicketUtils to the exact semantics of the replaced
 * io.trino.plugin.base.authentication.KerberosTicketUtils (P3b-T01 commit 1, trino -> JDK).
 */
public class KerberosTicketUtilsTest {

    private static final String REALM = "EXAMPLE.COM";

    private static KerberosTicket ticket(String serverPrincipal, long startMs, long endMs) {
        return new KerberosTicket(
                new byte[] {1},                                  // asn1Encoding (non-empty)
                new KerberosPrincipal("client@" + REALM),        // client
                new KerberosPrincipal(serverPrincipal),          // server
                new byte[] {0, 0, 0, 0, 0, 0, 0, 0},             // sessionKey
                1,                                               // keyType
                null,                                            // flags
                null,                                            // authTime
                new Date(startMs),                               // startTime
                new Date(endMs),                                 // endTime
                null,                                            // renewTill
                null);                                           // clientAddresses
    }

    // getRefreshTime == start + (long)((end - start) * 0.8f) -- the trino 80% renew window.
    @Test
    public void testGetRefreshTimeIs80PercentOfLifetime() {
        KerberosTicket tgt = ticket("krbtgt/" + REALM + "@" + REALM, 1000L, 11000L);
        // 1000 + (long)((11000 - 1000) * 0.8f) = 1000 + 8000 = 9000
        Assert.assertEquals(9000L, KerberosTicketUtils.getRefreshTime(tgt));
    }

    @Test
    public void testGetRefreshTimeZeroLifetime() {
        KerberosTicket tgt = ticket("krbtgt/" + REALM + "@" + REALM, 5000L, 5000L);
        Assert.assertEquals(5000L, KerberosTicketUtils.getRefreshTime(tgt));
    }

    // getTicketGrantingTicket returns the credential whose server is krbtgt/REALM@REALM.
    @Test
    public void testGetTicketGrantingTicketPicksTgtAmongCredentials() {
        KerberosTicket serviceTicket = ticket("hdfs/host@" + REALM, 0L, 10000L);
        KerberosTicket tgt = ticket("krbtgt/" + REALM + "@" + REALM, 0L, 10000L);
        Set<Object> privateCreds = new HashSet<>();
        privateCreds.add(serviceTicket);
        privateCreds.add(tgt);
        Subject subject = new Subject(false,
                Collections.singleton(new KerberosPrincipal("client@" + REALM)),
                Collections.emptySet(), privateCreds);

        Assert.assertSame(tgt, KerberosTicketUtils.getTicketGrantingTicket(subject));
    }

    @Test
    public void testGetTicketGrantingTicketThrowsWhenNoTgt() {
        KerberosTicket serviceTicket = ticket("hdfs/host@" + REALM, 0L, 10000L);
        Subject subject = new Subject(false,
                Collections.singleton(new KerberosPrincipal("client@" + REALM)),
                Collections.emptySet(), Collections.singleton(serviceTicket));
        try {
            KerberosTicketUtils.getTicketGrantingTicket(subject);
            Assert.fail("expected IllegalArgumentException when no TGT is present");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}

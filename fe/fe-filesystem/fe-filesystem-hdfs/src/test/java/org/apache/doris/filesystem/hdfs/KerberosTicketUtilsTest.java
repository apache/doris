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

package org.apache.doris.filesystem.hdfs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

class KerberosTicketUtilsTest {

    private static final KerberosPrincipal CLIENT = new KerberosPrincipal("doris/test@EXAMPLE.COM");
    private static final KerberosPrincipal TGS = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
    private static final KerberosPrincipal SERVICE = new KerberosPrincipal("hive/host@EXAMPLE.COM");

    static KerberosTicket ticket(KerberosPrincipal server, long startMillis, long endMillis) {
        return new KerberosTicket(new byte[] {1}, CLIENT, server, new byte[] {1}, 1, null,
                new Date(startMillis), new Date(startMillis), new Date(endMillis), null, null);
    }

    @Test
    void getRefreshTimeIsAt80PercentOfTicketLifetime() {
        long start = 1_000_000L;
        long end = start + 100_000L;
        Assertions.assertEquals(start + 80_000L,
                KerberosTicketUtils.getRefreshTime(ticket(TGS, start, end)));
    }

    @Test
    void getTicketGrantingTicketSelectsTgtAmongOtherTickets() {
        KerberosTicket serviceTicket = ticket(SERVICE, 0, 100_000);
        KerberosTicket tgt = ticket(TGS, 0, 100_000);
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(serviceTicket);
        subject.getPrivateCredentials().add(tgt);
        Assertions.assertSame(tgt, KerberosTicketUtils.getTicketGrantingTicket(subject));
    }

    @Test
    void getTicketGrantingTicketThrowsWhenAbsent() {
        Subject subject = new Subject();
        subject.getPrivateCredentials().add(ticket(SERVICE, 0, 100_000));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> KerberosTicketUtils.getTicketGrantingTicket(subject));
    }

    @Test
    void isOriginalTicketGrantingTicketChecksServerPrincipalForm() {
        Assertions.assertTrue(
                KerberosTicketUtils.isOriginalTicketGrantingTicket(ticket(TGS, 0, 100_000)));
        Assertions.assertFalse(
                KerberosTicketUtils.isOriginalTicketGrantingTicket(ticket(SERVICE, 0, 100_000)));
    }
}

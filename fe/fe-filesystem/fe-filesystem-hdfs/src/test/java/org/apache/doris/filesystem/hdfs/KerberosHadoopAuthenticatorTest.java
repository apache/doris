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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * Unit tests for the proactive TGT refresh logic. No KDC: the JAAS keytab login
 * (package-private {@code loginSubject()} seam, trino's KerberosAuthentication
 * boundary) is replaced with fabricated Subjects holding real KerberosTicket
 * objects whose start/end times steer the 80%-lifetime refresh point.
 */
class KerberosHadoopAuthenticatorTest {

    private static final KerberosPrincipal CLIENT = new KerberosPrincipal("doris/test@EXAMPLE.COM");
    private static final KerberosPrincipal TGS = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");

    /** Canned-login subclass; static state because loginSubject() is called from the super constructor. */
    private static final class FakeLoginAuthenticator extends KerberosHadoopAuthenticator {
        static final Deque<Subject> LOGINS = new ArrayDeque<>();
        static int loginCount = 0;
        static RuntimeException nextLoginFailure = null;

        FakeLoginAuthenticator() {
            super("doris/test@EXAMPLE.COM", "/path/to/doris.keytab", new Configuration());
        }

        @Override
        Subject loginSubject() {
            loginCount++;
            if (nextLoginFailure != null) {
                RuntimeException failure = nextLoginFailure;
                nextLoginFailure = null;
                throw failure;
            }
            return LOGINS.removeFirst();
        }
    }

    private static Subject subjectWithTgt(long startMillis, long endMillis) {
        Subject subject = new Subject();
        subject.getPrincipals().add(CLIENT);
        subject.getPrivateCredentials().add(new KerberosTicket(new byte[] {1}, CLIENT, TGS,
                new byte[] {1}, 1, null, new Date(startMillis), new Date(startMillis),
                new Date(endMillis), null, null));
        return subject;
    }

    @BeforeEach
    void resetFakeLogins() {
        FakeLoginAuthenticator.LOGINS.clear();
        FakeLoginAuthenticator.loginCount = 0;
        FakeLoginAuthenticator.nextLoginFailure = null;
    }

    @Test
    void constructorLogsInEagerlyAndFreshTicketIsNotRefreshed() throws IOException {
        long now = System.currentTimeMillis();
        // refresh point = now + 0.8 * 600s = now + 480s, far in the future
        FakeLoginAuthenticator.LOGINS.add(subjectWithTgt(now, now + 600_000));

        KerberosHadoopAuthenticator auth = new FakeLoginAuthenticator();
        Assertions.assertEquals(1, FakeLoginAuthenticator.loginCount);

        Assertions.assertEquals("ok", auth.doAs(() -> "ok"));
        auth.doAs(() -> "ok");
        Assertions.assertEquals(1, FakeLoginAuthenticator.loginCount);
    }

    @Test
    void staleTicketIsRefreshedInPlaceWithoutThrottle() throws IOException {
        long now = System.currentTimeMillis();
        // initial TGT is past its 80%-lifetime refresh point:
        // refresh point = (now-100s) + 0.8 * 101s = now - 19.2s < now
        Subject initial = subjectWithTgt(now - 100_000, now + 1_000);
        Subject renewed = subjectWithTgt(now, now + 600_000);
        FakeLoginAuthenticator.LOGINS.add(initial);
        FakeLoginAuthenticator.LOGINS.add(renewed);

        KerberosHadoopAuthenticator auth = new FakeLoginAuthenticator();
        Assertions.assertEquals(1, FakeLoginAuthenticator.loginCount);

        // constructor login happened seconds ago; a 60s-throttled implementation
        // (checkTGTAndReloginFromKeytab) would skip this refresh — ours must not
        Assertions.assertEquals("ok", auth.doAs(() -> "ok"));
        Assertions.assertEquals(2, FakeLoginAuthenticator.loginCount);

        // in-place swap: the ORIGINAL Subject now holds the renewed TGT
        KerberosTicket current = KerberosTicketUtils.getTicketGrantingTicket(initial);
        Assertions.assertEquals(now + 600_000, current.getEndTime().getTime());

        // renewed ticket is fresh → no further login
        auth.doAs(() -> "ok");
        Assertions.assertEquals(2, FakeLoginAuthenticator.loginCount);
    }

    @Test
    void doAsPropagatesIOException() {
        long now = System.currentTimeMillis();
        FakeLoginAuthenticator.LOGINS.add(subjectWithTgt(now, now + 600_000));

        KerberosHadoopAuthenticator auth = new FakeLoginAuthenticator();
        IOException thrown = Assertions.assertThrows(IOException.class, () -> auth.doAs(() -> {
            throw new IOException("intentional");
        }));
        Assertions.assertEquals("intentional", thrown.getMessage());
    }

    @Test
    void constructorWrapsLoginFailureWithPrincipalAndKeytab() {
        FakeLoginAuthenticator.nextLoginFailure =
                new RuntimeException(new javax.security.auth.login.LoginException("no keytab"));
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class,
                FakeLoginAuthenticator::new);
        Assertions.assertTrue(thrown.getMessage().contains("doris/test@EXAMPLE.COM"));
        Assertions.assertTrue(thrown.getMessage().contains("/path/to/doris.keytab"));
    }

    @Test
    void doAsWrapsRefreshLoginFailureAsIOException() throws IOException {
        long now = System.currentTimeMillis();
        // TGT already past its 80% refresh point, so the first doAs attempts a relogin
        FakeLoginAuthenticator.LOGINS.add(subjectWithTgt(now - 100_000, now + 1_000));

        KerberosHadoopAuthenticator auth = new FakeLoginAuthenticator();
        FakeLoginAuthenticator.nextLoginFailure =
                new RuntimeException(new javax.security.auth.login.LoginException("kdc down"));

        IOException thrown = Assertions.assertThrows(IOException.class, () -> auth.doAs(() -> "ok"));
        Assertions.assertTrue(thrown.getMessage().contains("Kerberos relogin failed"));
        Assertions.assertTrue(thrown.getMessage().contains("doris/test@EXAMPLE.COM"));

        // a later doAs retries the login (nextRefreshTime was not advanced) and succeeds
        FakeLoginAuthenticator.LOGINS.add(subjectWithTgt(now, now + 600_000));
        Assertions.assertEquals("ok", auth.doAs(() -> "ok"));
    }
}

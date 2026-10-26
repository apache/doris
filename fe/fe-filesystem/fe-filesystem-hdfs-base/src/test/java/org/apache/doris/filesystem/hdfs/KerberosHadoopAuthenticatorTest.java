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

import org.apache.doris.kerberos.HadoopKerberosAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

class KerberosHadoopAuthenticatorTest {
    private String originalRealm;
    private String originalKdc;

    @BeforeEach
    void setUp() {
        originalRealm = System.getProperty("java.security.krb5.realm");
        originalKdc = System.getProperty("java.security.krb5.kdc");
        System.setProperty("java.security.krb5.realm", "DEFAULT.EXAMPLE");
        System.setProperty("java.security.krb5.kdc", "localhost");
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(configuration("FIRST.EXAMPLE"));
    }

    @AfterEach
    void tearDown() {
        UserGroupInformation.reset();
        restoreProperty("java.security.krb5.realm", originalRealm);
        restoreProperty("java.security.krb5.kdc", originalKdc);
    }

    @Test
    void filesystemAndConnectorRealmsSurviveTicketRefresh() throws Exception {
        KerberosHadoopAuthenticator first = authenticator("FIRST.EXAMPLE", new AtomicInteger());
        AtomicInteger secondLogins = new AtomicInteger();
        KerberosHadoopAuthenticator second = Assertions.assertDoesNotThrow(
                () -> authenticator("SECOND.EXAMPLE", secondLogins));
        Assertions.assertEquals(1, secondLogins.get());
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("THIRD.EXAMPLE"));

        // The fabricated TGTs have expired, so execute() must rebuild the UGI before running the action.
        Assertions.assertEquals("hdfs/host@FIRST.EXAMPLE",
                first.execute(() -> UserGroupInformation.getCurrentUser().getUserName()));
        Assertions.assertEquals("hdfs/host@SECOND.EXAMPLE",
                second.execute(() -> UserGroupInformation.getCurrentUser().getUserName()));
        Assertions.assertEquals(2, secondLogins.get());
        Subject third = subject("THIRD.EXAMPLE");
        Assertions.assertEquals("hdfs", UserGroupInformation.getUGIFromSubject(third).getShortUserName());
    }

    private static KerberosHadoopAuthenticator authenticator(String realm, AtomicInteger logins) {
        return new KerberosHadoopAuthenticator("hdfs/host@" + realm, "unused.keytab", configuration(realm)) {
            @Override
            Subject loginSubject() {
                logins.incrementAndGet();
                return subject(realm);
            }
        };
    }

    private static Subject subject(String realm) {
        KerberosPrincipal client = new KerberosPrincipal("hdfs/host@" + realm);
        KerberosTicket ticket = new KerberosTicket(new byte[] {1}, client,
                new KerberosPrincipal("krbtgt/" + realm + "@" + realm), new byte[8], 1,
                null, null, new Date(1000L), new Date(2000L), null, null);
        return new Subject(false, Set.of(client), Set.of(), Set.of(ticket));
    }

    private static Configuration configuration(String realm) {
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.auth_to_local", "RULE:[2:$1@$0](.*@" + realm + ")s/@.*// DEFAULT");
        return conf;
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}

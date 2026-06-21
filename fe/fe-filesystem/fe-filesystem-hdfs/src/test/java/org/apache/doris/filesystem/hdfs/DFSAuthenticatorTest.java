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

import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.kerberos.HadoopKerberosAuthenticator;
import org.apache.doris.kerberos.HadoopSimpleAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that {@link DFSFileSystem} wires the shared {@code fe-kerberos} authenticators
 * (P3b consolidation): the duplicate fe-filesystem-hdfs {@code KerberosHadoopAuthenticator}/
 * {@code SimpleHadoopAuthenticator} copies are gone.
 *
 * <p>It also pins the deliberately-adopted behavior change: the simple/no-username path now
 * executes as remote user {@code "hadoop"} (legacy fe-common/HMS parity) instead of the FE
 * process user. The duplicate copy used to run such actions directly as the FE process user.
 */
class DFSAuthenticatorTest {

    @Test
    void simpleNoUsernameRunsAsHadoop() throws IOException {
        HadoopAuthenticator auth = DFSFileSystem.buildAuthenticator(new HashMap<>(), new Configuration());
        Assertions.assertTrue(auth instanceof HadoopSimpleAuthenticator,
                "non-kerberos properties must select the shared simple authenticator");
        Assertions.assertEquals("hadoop", auth.getUGI().getUserName(),
                "no hadoop.username must default to remote user 'hadoop' (fe-common/HMS parity)");
    }

    @Test
    void simpleWithUsernameRunsAsThatUser() throws IOException {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.username", "testuser");
        HadoopAuthenticator auth = DFSFileSystem.buildAuthenticator(props, new Configuration());
        Assertions.assertTrue(auth instanceof HadoopSimpleAuthenticator);
        Assertions.assertEquals("testuser", auth.getUGI().getUserName(),
                "hadoop.username must drive the simple authenticator's UGI identity");
    }

    @Test
    void simpleEmptyUsernameRunsAsHadoop() throws IOException {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.username", "");
        HadoopAuthenticator auth = DFSFileSystem.buildAuthenticator(props, new Configuration());
        Assertions.assertTrue(auth instanceof HadoopSimpleAuthenticator);
        // Empty must be treated like absent (-> remote user "hadoop"), not passed to
        // UserGroupInformation.createRemoteUser("") which would throw IllegalArgumentException.
        Assertions.assertEquals("hadoop", auth.getUGI().getUserName(),
                "empty hadoop.username must default to remote user 'hadoop', not crash construction");
    }

    @Test
    void principalAndKeytabSelectKerberos() {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.kerberos.principal", "doris/host@EXAMPLE.COM");
        props.put("hadoop.kerberos.keytab", "/etc/doris.keytab");
        HadoopAuthenticator auth = DFSFileSystem.buildAuthenticator(props, new Configuration());
        // Type only: getUGI() would trigger a real KDC login (covered by docker kerberos e2e).
        Assertions.assertTrue(auth instanceof HadoopKerberosAuthenticator,
                "principal + keytab presence must select the shared kerberos authenticator");
    }
}

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

import org.apache.doris.filesystem.Location;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Environment-dependent integration tests for {@link KerberosHadoopAuthenticator}.
 * Requires a Kerberos KDC and a Kerberized HDFS cluster.
 */
@Tag("environment")
@Tag("kerberos")
@EnabledIfEnvironmentVariable(named = "DORIS_FS_TEST_KDC_PRINCIPAL", matches = ".+")
class KerberosHadoopAuthenticatorEnvTest {

    private static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return val;
    }

    private static Configuration kerberosConf() {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        String hdfsHost = System.getenv("DORIS_FS_TEST_HDFS_HOST");
        String hdfsPort = System.getenv("DORIS_FS_TEST_HDFS_PORT");
        if (hdfsHost != null && hdfsPort != null) {
            conf.set("fs.defaultFS", "hdfs://" + hdfsHost + ":" + hdfsPort);
        }
        return conf;
    }

    @Test
    void loginSucceeds() {
        String principal = requireEnv("DORIS_FS_TEST_KDC_PRINCIPAL");
        String keytab = requireEnv("DORIS_FS_TEST_KDC_KEYTAB");

        KerberosHadoopAuthenticator auth =
                new KerberosHadoopAuthenticator(principal, keytab, kerberosConf());
        Assertions.assertNotNull(auth);
    }

    @Test
    void doAsExecutesAction() throws IOException {
        String principal = requireEnv("DORIS_FS_TEST_KDC_PRINCIPAL");
        String keytab = requireEnv("DORIS_FS_TEST_KDC_KEYTAB");

        KerberosHadoopAuthenticator auth =
                new KerberosHadoopAuthenticator(principal, keytab, kerberosConf());
        String result = auth.doAs(() -> "hello-from-kerberos");
        Assertions.assertEquals("hello-from-kerberos", result);
    }

    @Test
    void doAsPropagatesIOException() {
        String principal = requireEnv("DORIS_FS_TEST_KDC_PRINCIPAL");
        String keytab = requireEnv("DORIS_FS_TEST_KDC_KEYTAB");

        KerberosHadoopAuthenticator auth =
                new KerberosHadoopAuthenticator(principal, keytab, kerberosConf());
        Assertions.assertThrows(IOException.class, () -> auth.doAs(() -> {
            throw new IOException("intentional");
        }));
    }

    @Test
    void hdfsOperationWithKerberos() throws IOException {
        String principal = requireEnv("DORIS_FS_TEST_KDC_PRINCIPAL");
        String keytab = requireEnv("DORIS_FS_TEST_KDC_KEYTAB");
        String hdfsHost = requireEnv("DORIS_FS_TEST_HDFS_HOST");
        String hdfsPort = System.getenv("DORIS_FS_TEST_HDFS_PORT");
        if (hdfsPort == null || hdfsPort.isEmpty()) {
            hdfsPort = "8020";
        }

        Map<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs://" + hdfsHost + ":" + hdfsPort);
        props.put("hadoop.security.authentication", "kerberos");
        props.put("dfs.namenode.kerberos.principal", principal);
        props.put("hadoop.kerberos.principal", principal);
        props.put("hadoop.kerberos.keytab", keytab);

        try (DFSFileSystem dfs = new DFSFileSystem(props)) {
            boolean exists = dfs.exists(Location.of("hdfs://" + hdfsHost + ":" + hdfsPort + "/"));
            Assertions.assertTrue(exists, "Root directory should exist");
        }
    }
}

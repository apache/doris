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

package org.apache.doris.common.security.authentication;

import org.apache.doris.common.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class AuthenticationTest {

    @Test
    public void testAuthConf() {
        // simple
        Configuration conf = new Configuration();
        AuthenticationConfig conf1 = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(SimpleAuthenticationConfig.class, conf1.getClass());

        // simple
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        AuthenticationConfig conf2 = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(SimpleAuthenticationConfig.class, conf2.getClass());

        // kerberos will be tested below
    }

    @Test
    public void testSetKerberosKeytabWithAbsolutePath() throws IOException {
        // Create a temporary keytab file
        File tempKeytab = File.createTempFile("test", ".keytab");
        tempKeytab.deleteOnExit();

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        conf.set(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL, "principal");
        conf.set(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB, tempKeytab.getAbsolutePath().toString());
        AuthenticationConfig authConf = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(KerberosAuthenticationConfig.class, authConf.getClass());

        KerberosAuthenticationConfig config = (KerberosAuthenticationConfig) authConf;
        Assert.assertEquals(tempKeytab.getAbsolutePath(), config.getKerberosKeytab());
    }

    @Test(expected = RuntimeException.class)
    public void testSetKerberosKeytabWithNonExistentAbsolutePath() {
        KerberosAuthenticationConfig config = new KerberosAuthenticationConfig();
        config.setKerberosKeytab("/non/existent/path/test.keytab");
    }

    @Test
    public void testSetKerberosKeytabWithRelativePath() throws IOException {
        // Save original custom_config_dir value
        String originalConfigDir = Config.custom_config_dir;

        try {
            // Create a temporary directory to serve as custom_config_dir
            File tempDir = Files.createTempDirectory("test_config").toFile();
            tempDir.deleteOnExit();

            // Create a keytab file in the temporary directory
            File tempKeytab = new File(tempDir, "test.keytab");
            tempKeytab.createNewFile();
            tempKeytab.deleteOnExit();

            // Set custom_config_dir to our temporary directory
            Config.custom_config_dir = tempDir.getAbsolutePath();

            KerberosAuthenticationConfig config = new KerberosAuthenticationConfig();
            config.setKerberosKeytab("test.keytab");

            Assert.assertEquals(tempKeytab.getAbsolutePath(), config.getKerberosKeytab());
        } finally {
            // Restore original custom_config_dir
            Config.custom_config_dir = originalConfigDir;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testSetKerberosKeytabWithNonExistentRelativePath() throws IOException {
        // Save original custom_config_dir value
        String originalConfigDir = Config.custom_config_dir;

        try {
            // Create a temporary directory to serve as custom_config_dir
            File tempDir = Files.createTempDirectory("test_config").toFile();
            tempDir.deleteOnExit();

            // Set custom_config_dir to our temporary directory
            Config.custom_config_dir = tempDir.getAbsolutePath();

            KerberosAuthenticationConfig config = new KerberosAuthenticationConfig();
            config.setKerberosKeytab("non_existent.keytab");
        } finally {
            // Restore original custom_config_dir
            Config.custom_config_dir = originalConfigDir;
        }
    }
}

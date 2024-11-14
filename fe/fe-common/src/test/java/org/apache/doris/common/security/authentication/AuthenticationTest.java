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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Assert;
import org.junit.Test;

public class AuthenticationTest {

    @Test
    public void testAuthConf() {
        Configuration conf = new Configuration();

        AuthenticationConfig conf1 = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(SimpleAuthenticationConfig.class, conf1.getClass());

        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");

        AuthenticationConfig conf2 = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(SimpleAuthenticationConfig.class, conf2.getClass());

        conf.set(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL, "principal");
        conf.set(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB, "keytab");

        AuthenticationConfig conf3 = AuthenticationConfig.getKerberosConfig(conf);
        Assert.assertEquals(KerberosAuthenticationConfig.class, conf3.getClass());
    }
}

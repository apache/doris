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

public abstract class AuthenticationConfig {
    public static String HADOOP_USER_NAME = "hadoop.username";
    public static String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    public static String HIVE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    public static String HIVE_KERBEROS_KEYTAB = "hive.metastore.kerberos.keytab.file";
    public static String DORIS_KRB5_DEBUG = "doris.krb5.debug";

    /**
     * @return true if the config is valid, otherwise false.
     */
    public abstract boolean isValid();

    /**
     * get kerberos config from hadoop conf
     * @param conf config
     * @return ugi
     */
    public static AuthenticationConfig getKerberosConfig(Configuration conf) {
        return AuthenticationConfig.getKerberosConfig(conf, HADOOP_KERBEROS_PRINCIPAL, HADOOP_KERBEROS_KEYTAB);
    }

    /**
     * get kerberos config from hadoop conf
     * @param conf config
     * @param krbPrincipalKey principal key
     * @param krbKeytabKey keytab key
     * @return ugi
     */
    public static AuthenticationConfig getKerberosConfig(Configuration conf,
                                                         String krbPrincipalKey,
                                                         String krbKeytabKey) {
        String authentication = conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, null);
        if (AuthType.KERBEROS.getDesc().equals(authentication)) {
            KerberosAuthenticationConfig krbConfig = new KerberosAuthenticationConfig();
            krbConfig.setKerberosPrincipal(conf.get(krbPrincipalKey));
            krbConfig.setKerberosKeytab(conf.get(krbKeytabKey));
            krbConfig.setConf(conf);
            krbConfig.setPrintDebugLog(Boolean.parseBoolean(conf.get(DORIS_KRB5_DEBUG, "false")));
            return krbConfig;
        } else {
            // AuthType.SIMPLE
            SimpleAuthenticationConfig simpleAuthenticationConfig = new SimpleAuthenticationConfig();
            simpleAuthenticationConfig.setUsername(conf.get(HADOOP_USER_NAME));
            return simpleAuthenticationConfig;
        }
    }
}

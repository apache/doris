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

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AuthenticationConfig {
    private static final Logger LOG = LogManager.getLogger(AuthenticationConfig.class);
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

    public static AuthenticationConfig getSimpleAuthenticationConfig(Configuration conf) {
        return AuthenticationConfig.createSimpleAuthenticationConfig(conf);
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
            String principalKey = conf.get(krbPrincipalKey);
            String keytabKey = conf.get(krbKeytabKey);
            if (!Strings.isNullOrEmpty(principalKey) && !Strings.isNullOrEmpty(keytabKey)) {
                KerberosAuthenticationConfig krbConfig = new KerberosAuthenticationConfig();
                krbConfig.setKerberosPrincipal(principalKey);
                krbConfig.setKerberosKeytab(keytabKey);
                krbConfig.setConf(conf);
                krbConfig.setPrintDebugLog(Boolean.parseBoolean(conf.get(DORIS_KRB5_DEBUG, "false")));
                return krbConfig;
            } else {
                // Due to some historical reasons, `core-size.xml` may be stored in path:`fe/conf`,
                // but this file may only contain `hadoop.security.authentication configuration`,
                // and no krbPrincipalKey and krbKeytabKey,
                // which will cause kerberos initialization failure.
                // Now:
                //   if kerberos is needed, the relevant configuration can be written in the catalog properties,
                //   if kerberos is not needed, to prevent the influence of historical reasons,
                //      the following simple authentication method needs to be used.
                LOG.warn("{} or {} is null or empty, fallback to simple authentication",
                        krbPrincipalKey, krbKeytabKey);
            }
        }
        return createSimpleAuthenticationConfig(conf);
    }

    private static AuthenticationConfig createSimpleAuthenticationConfig(Configuration conf) {
        // AuthType.SIMPLE
        SimpleAuthenticationConfig simpleAuthenticationConfig = new SimpleAuthenticationConfig();
        simpleAuthenticationConfig.setUsername(conf.get(HADOOP_USER_NAME));
        return simpleAuthenticationConfig;
    }
}

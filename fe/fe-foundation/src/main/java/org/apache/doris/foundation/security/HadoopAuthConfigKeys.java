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

package org.apache.doris.foundation.security;

/**
 * Names of the Hadoop authentication properties a user writes in a catalog, resource or storage
 * vault definition.
 *
 * <p>These are plain strings that the engine and every plugin have to spell the same way, while the
 * code turning them into a Hadoop {@code UserGroupInformation} lives in {@code fe-kerberos} and
 * links against Hadoop. Holding the names here lets a module read or write such a property without
 * Hadoop on its classpath — and, because {@code AuthenticationConfig}'s copies are not compile-time
 * constants, without loading that class (and with it Hadoop's {@code Configuration}) at runtime
 * either. {@code org.apache.doris.kerberos.AuthenticationConfig} re-exports every constant declared
 * here, so both spellings name the same property.
 */
public final class HadoopAuthConfigKeys {
    public static final String HADOOP_USER_NAME = "hadoop.username";
    public static final String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static final String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    public static final String HADOOP_SECURITY_AUTH_TO_LOCAL = "hadoop.security.auth_to_local";
    public static final String DORIS_KRB5_DEBUG = "doris.krb5.debug";

    private HadoopAuthConfigKeys() {
    }
}

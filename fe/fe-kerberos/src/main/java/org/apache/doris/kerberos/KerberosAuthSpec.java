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

package org.apache.doris.kerberos;

import java.util.Objects;

/**
 * Neutral, immutable carrier of the Kerberos login facts (client {@code principal} and
 * {@code keytab}) needed to perform a {@code UGI.loginUserFromKeytab(...).doAs(...)}.
 *
 * <p>This is a fact object only — it holds no Hadoop types and performs no login. The
 * real authenticated execution is done elsewhere (the FE side, via
 * {@code ConnectorContext.executeAuthenticated}). It mirrors the principal/keytab pair of
 * fe-common {@code KerberosAuthenticationConfig} but stays dependency-free so connector
 * and metastore-api modules can pass it around without pulling fe-common or Hadoop.
 *
 * <p>The HMS <em>service</em> principal (e.g. {@code hive.metastore.kerberos.principal}) is
 * deliberately NOT part of this spec: it is a HiveConf override carried via
 * {@code HmsMetaStoreProperties.toHiveConfOverrides(String)}, not a doAs login fact.
 */
public final class KerberosAuthSpec {

    private final String principal;
    private final String keytab;

    public KerberosAuthSpec(String principal, String keytab) {
        this.principal = principal;
        this.keytab = keytab;
    }

    /** The Kerberos client principal used for the keytab login. */
    public String getPrincipal() {
        return principal;
    }

    /** The path to the keytab file used for the principal login. */
    public String getKeytab() {
        return keytab;
    }

    /** Returns true only when both principal and keytab are non-blank (a usable login pair). */
    public boolean hasCredentials() {
        return isNotBlank(principal) && isNotBlank(keytab);
    }

    private static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KerberosAuthSpec)) {
            return false;
        }
        KerberosAuthSpec that = (KerberosAuthSpec) o;
        return Objects.equals(principal, that.principal) && Objects.equals(keytab, that.keytab);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principal, keytab);
    }

    @Override
    public String toString() {
        return "KerberosAuthSpec{principal=" + principal + ", keytab=" + keytab + "}";
    }
}

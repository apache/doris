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

package org.apache.doris.connector.metastore;

import org.apache.doris.kerberos.AuthType;
import org.apache.doris.kerberos.KerberosAuthSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

/**
 * Contract tests for the neutral metastore API. These pin the documented capability defaults and
 * verify the HMS sub-interface carries the fe-kerberos facts — i.e. that the api compiles against
 * and integrates the {@code fe-kerberos} {@link AuthType}/{@link KerberosAuthSpec} types.
 */
class MetaStorePropertiesContractTest {

    /** Minimal MetaStoreProperties that overrides nothing, to exercise the default methods. */
    private static class BareMetaStore implements MetaStoreProperties {
        @Override
        public String providerName() {
            return "BARE";
        }

        @Override
        public Map<String, String> rawProperties() {
            return Map.of("k", "v");
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Map.of();
        }
    }

    @Test
    void capabilityDefaults_areConservative() {
        // Intent (D-006 / §1.4): a backend opts IN to needing storage / vended credentials; the
        // safe default for both is false so HMS/REST/JDBC do not pull storage they do not use.
        MetaStoreProperties ms = new BareMetaStore();

        Assertions.assertEquals("BARE", ms.providerName());
        Assertions.assertFalse(ms.needsStorage());
        Assertions.assertFalse(ms.needsVendedCredentials());
        Assertions.assertDoesNotThrow(ms::validate);
        Assertions.assertEquals("v", ms.rawProperties().get("k"));
        Assertions.assertTrue(ms.matchedProperties().isEmpty());
    }

    @Test
    void capabilities_canBeOverridden() {
        MetaStoreProperties needsBoth = new BareMetaStore() {
            @Override
            public boolean needsStorage() {
                return true;
            }

            @Override
            public boolean needsVendedCredentials() {
                return true;
            }
        };

        Assertions.assertTrue(needsBoth.needsStorage());
        Assertions.assertTrue(needsBoth.needsVendedCredentials());
    }

    @Test
    void hmsSubInterface_carriesNeutralKerberosFacts() {
        KerberosAuthSpec spec = new KerberosAuthSpec("hive/_HOST@REALM", "/etc/hive.keytab");
        HmsMetaStoreProperties hms = new HmsMetaStoreProperties() {
            @Override
            public String providerName() {
                return "HMS";
            }

            @Override
            public Map<String, String> rawProperties() {
                return Map.of();
            }

            @Override
            public Map<String, String> matchedProperties() {
                return Map.of();
            }

            @Override
            public String getUri() {
                return "thrift://hms:9083";
            }

            @Override
            public AuthType getAuthType() {
                return AuthType.KERBEROS;
            }

            @Override
            public Map<String, String> toHiveConfOverrides(String defaultClientSocketTimeoutSeconds) {
                return Map.of("hive.metastore.sasl.enabled", "true");
            }

            @Override
            public Optional<KerberosAuthSpec> kerberos() {
                return Optional.of(spec);
            }
        };

        Assertions.assertEquals("thrift://hms:9083", hms.getUri());
        Assertions.assertEquals(AuthType.KERBEROS, hms.getAuthType());
        Assertions.assertEquals("true", hms.toHiveConfOverrides("10").get("hive.metastore.sasl.enabled"));
        Assertions.assertTrue(hms.kerberos().isPresent());
        Assertions.assertTrue(hms.kerberos().get().hasCredentials());
        Assertions.assertEquals("hive/_HOST@REALM", hms.kerberos().get().getPrincipal());
    }
}

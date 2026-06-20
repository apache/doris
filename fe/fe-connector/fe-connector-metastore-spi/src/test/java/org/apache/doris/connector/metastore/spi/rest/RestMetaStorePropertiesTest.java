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

package org.apache.doris.connector.metastore.spi.rest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** T2 parity for the REST backend (legacy {@code PaimonRestMetaStoreProperties} / {@code appendRestOptions}). */
public class RestMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    public void toRestOptionsStripsPaimonRestPrefix() {
        RestMetaStorePropertiesImpl props = RestMetaStorePropertiesImpl.of(raw(
                "paimon.rest.uri", "http://rest:8080",
                "paimon.rest.token.provider", "dlf",
                "warehouse", "wh"));

        Assertions.assertEquals("REST", props.providerName());
        Assertions.assertFalse(props.needsStorage());
        Assertions.assertEquals("http://rest:8080", props.getUri());

        Map<String, String> opts = props.toRestOptions();
        Assertions.assertEquals("http://rest:8080", opts.get("uri"));
        Assertions.assertEquals("dlf", opts.get("token.provider"));
        // Raw catalog keys (warehouse) are NOT REST options.
        Assertions.assertFalse(opts.containsKey("warehouse"));
    }

    @Test
    public void uriAliasResolvesFromPlainUri() {
        RestMetaStorePropertiesImpl props = RestMetaStorePropertiesImpl.of(raw("uri", "http://plain", "warehouse", "wh"));
        Assertions.assertEquals("http://plain", props.getUri());
        Assertions.assertEquals("http://plain", props.toRestOptions().get("uri"));
    }

    @Test
    public void validateChecksWarehouseThenUriThenDlfToken() {
        // warehouse first (shared, legacy parity)
        Assertions.assertEquals("Property warehouse is required.",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> RestMetaStorePropertiesImpl.of(raw("paimon.rest.uri", "http://r")).validate())
                        .getMessage());
        // then uri
        Assertions.assertEquals("paimon.rest.uri or uri is required",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> RestMetaStorePropertiesImpl.of(raw("warehouse", "wh")).validate()).getMessage());
        // then the DLF token-provider rule
        Assertions.assertEquals("DLF token provider requires 'paimon.rest.dlf.access-key-id' "
                        + "and 'paimon.rest.dlf.access-key-secret'",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> RestMetaStorePropertiesImpl.of(raw(
                                "warehouse", "wh", "paimon.rest.uri", "http://r",
                                "paimon.rest.token.provider", "dlf")).validate()).getMessage());
        // valid (dlf token with both keys) -> no throw
        RestMetaStorePropertiesImpl.of(raw(
                "warehouse", "wh", "paimon.rest.uri", "http://r", "paimon.rest.token.provider", "dlf",
                "paimon.rest.dlf.access-key-id", "id", "paimon.rest.dlf.access-key-secret", "secret")).validate();
    }

    @Test
    public void dlfTokenRuleIsCaseSensitiveMatchingLegacyParamRules() {
        // Legacy ParamRules.requireIf uses Objects.equals("dlf", tokenProvider) (case-sensitive), so an
        // uppercase "DLF" does NOT trigger the dlf-keys requirement. (The paimon hand-copy's equalsIgnoreCase
        // would throw here; we match the authoritative legacy contract.)
        RestMetaStorePropertiesImpl.of(raw(
                "warehouse", "wh", "paimon.rest.uri", "http://r", "paimon.rest.token.provider", "DLF")).validate();
    }
}

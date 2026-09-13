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

package org.apache.doris.connector.metastore.iceberg.glue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Parity for the iceberg Glue backend (legacy {@code AWSGlueMetaStoreBaseProperties.buildRules} +
 * {@code requireExplicitGlueCredentials}): verbatim §4 messages, in fire order
 * (AK/SK-together → endpoint-required → endpoint-https → at-least-one-credential). No warehouse rule.
 */
public class IcebergGlueMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String validateError(Map<String, String> raw) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergGlueMetaStoreProperties.of(raw).validate()).getMessage();
    }

    @Test
    public void rule1AccessKeyAndSecretMustBeSetTogether() {
        Assertions.assertEquals("glue.access_key and glue.secret_key must be set together",
                validateError(raw("glue.access_key", "ak")));
        Assertions.assertEquals("glue.access_key and glue.secret_key must be set together",
                validateError(raw("glue.secret_key", "sk")));
    }

    @Test
    public void rule2EndpointRequired() {
        // AK+SK present (rule 1 passes), endpoint blank => rule 2.
        Assertions.assertEquals("glue.endpoint must be set",
                validateError(raw("glue.access_key", "ak", "glue.secret_key", "sk")));
    }

    @Test
    public void rule3EndpointMustBeHttps() {
        Assertions.assertEquals("glue.endpoint must use https protocol,please set glue.endpoint to https://...",
                validateError(raw("glue.access_key", "ak", "glue.secret_key", "sk",
                        "glue.endpoint", "http://glue.us-east-1.amazonaws.com")));
    }

    @Test
    public void rule4AtLeastOneCredential() {
        // endpoint present + https, AK/SK both blank, role blank => requireExplicitGlueCredentials.
        Assertions.assertEquals("At least one of glue.access_key or glue.role_arn must be set",
                validateError(raw("glue.endpoint", "https://glue.us-east-1.amazonaws.com")));
    }

    @Test
    public void validWithAccessKeyOrRole() {
        Assertions.assertEquals("GLUE", IcebergGlueMetaStoreProperties.of(raw()).providerName());
        // AK + SK + https endpoint.
        IcebergGlueMetaStoreProperties.of(raw("glue.access_key", "ak", "glue.secret_key", "sk",
                "glue.endpoint", "https://glue.us-east-1.amazonaws.com")).validate();
        // role_arn alone (no AK/SK) + https endpoint: requireTogether passes (none present),
        // requireExplicitGlueCredentials satisfied by the role.
        IcebergGlueMetaStoreProperties.of(raw("glue.role_arn", "arn:aws:iam::1:role/r",
                "glue.endpoint", "https://glue.us-east-1.amazonaws.com")).validate();
    }

    @Test
    public void credentialAliasesResolve() {
        // aws.glue.access-key / aws.glue.secret-key are aliases for glue.access_key / glue.secret_key:
        // both set via aliases => requireTogether passes; the endpoint alias aws.endpoint resolves too.
        IcebergGlueMetaStoreProperties.of(raw("aws.glue.access-key", "ak", "aws.glue.secret-key", "sk",
                "aws.endpoint", "https://glue.us-east-1.amazonaws.com")).validate();
    }
}

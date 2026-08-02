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

package org.apache.doris.connector.metastore.iceberg.rest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Parity for the iceberg REST backend (legacy {@code IcebergRestProperties.initNormalizeAndCheckProps}):
 * verbatim §4 messages + the observable fire order (enum checks → eager body-throws → ParamRules).
 */
public class IcebergRestMetaStorePropertiesTest {

    private static Map<String, String> raw(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String validateError(Map<String, String> raw) {
        return Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergRestMetaStoreProperties.of(raw).validate()).getMessage();
    }

    @Test
    public void noneSecurityWithNoCredentialsIsValid() {
        // Defaults: security.type=none, credentials_provider_type=DEFAULT, no oauth2/signing/AK-SK.
        IcebergRestMetaStoreProperties.of(raw()).validate();
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.uri", "http://r")).validate();
        Assertions.assertEquals("REST", IcebergRestMetaStoreProperties.of(raw()).providerName());
    }

    @Test
    public void rule1InvalidSecurityType() {
        Assertions.assertEquals("Invalid security type: bogus. Supported values are: none, oauth2",
                validateError(raw("iceberg.rest.security.type", "bogus")));
        // case-insensitive accept (mirrors Security.valueOf(toUpperCase)).
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.security.type", "OAuth2",
                "iceberg.rest.oauth2.token", "t")).validate();
    }

    @Test
    public void rule2UnsupportedCredentialsProviderMode() {
        Assertions.assertEquals("Unsupported AWS credentials provider mode: bogus",
                validateError(raw("iceberg.rest.credentials_provider_type", "bogus")));
        // blank => DEFAULT (no throw); a known mode with '-' normalization is accepted.
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.credentials_provider_type", "")).validate();
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.credentials_provider_type", "instance-profile")).validate();
    }

    @Test
    public void rule3OAuth2ScopeOnlyWithCredentialNotToken() {
        Assertions.assertEquals("OAuth2 scope is only applicable when using credential, not token",
                validateError(raw("iceberg.rest.oauth2.token", "t", "iceberg.rest.oauth2.scope", "s")));
    }

    @Test
    public void rule4OAuth2RequiresCredentialOrToken() {
        Assertions.assertEquals("OAuth2 requires either credential or token",
                validateError(raw("iceberg.rest.security.type", "oauth2")));
        // satisfied by either credential or token.
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.security.type", "oauth2",
                "iceberg.rest.oauth2.credential", "c")).validate();
    }

    @Test
    public void rule5RoleArnRejected() {
        Assertions.assertEquals("iceberg.rest.role_arn is not supported for Iceberg REST catalog. "
                        + "Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, "
                        + "or iceberg.rest.credentials_provider_type instead",
                validateError(raw("iceberg.rest.role_arn", "arn:aws:iam::1:role/r")));
    }

    @Test
    public void rule6ExternalIdRejected() {
        Assertions.assertEquals("iceberg.rest.external-id is not supported for Iceberg REST catalog. "
                        + "Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, "
                        + "or iceberg.rest.credentials_provider_type instead",
                validateError(raw("iceberg.rest.external-id", "xyz")));
    }

    @Test
    public void rule7OAuth2MutuallyExclusiveCredentialAndToken() {
        // security.type defaults to none, so rule 4 does not fire; the mutuallyExclusive ParamRule does.
        Assertions.assertEquals("OAuth2 cannot have both credential and token configured",
                validateError(raw("iceberg.rest.oauth2.credential", "c", "iceberg.rest.oauth2.token", "t")));
    }

    @Test
    public void rule8And9SigningNameRequiresRegionAndSigV4() {
        Assertions.assertEquals(
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is glue",
                validateError(raw("iceberg.rest.signing-name", "glue")));
        Assertions.assertEquals(
                "Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is s3tables",
                validateError(raw("iceberg.rest.signing-name", "s3tables")));
        // satisfied when both region + sigv4-enabled present.
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.signing-name", "glue",
                "iceberg.rest.signing-region", "us-east-1", "iceberg.rest.sigv4-enabled", "true")).validate();
        // signing-name match is case-sensitive (ParamRules.requireIf uses Objects.equals): "Glue" != "glue"
        // so the rule does NOT fire. MUTATION: a case-insensitive match would throw here.
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.signing-name", "Glue")).validate();
    }

    @Test
    public void rule10AccessKeyAndSecretMustBeSetTogether() {
        Assertions.assertEquals("iceberg.rest.access-key-id and iceberg.rest.secret-access-key must be set together",
                validateError(raw("iceberg.rest.access-key-id", "ak")));
        Assertions.assertEquals("iceberg.rest.access-key-id and iceberg.rest.secret-access-key must be set together",
                validateError(raw("iceberg.rest.secret-access-key", "sk")));
        // both present => OK.
        IcebergRestMetaStoreProperties.of(raw("iceberg.rest.access-key-id", "ak",
                "iceberg.rest.secret-access-key", "sk")).validate();
    }

    @Test
    public void fireOrderSecurityTypeBeforeEverythingElse() {
        // WHY: rule 1 (security type) runs first. Even with a role_arn (rule 5) and an AK-only (rule 10)
        // also violated, the security-type error must surface. MUTATION: reordering checks would surface a
        // different message.
        Assertions.assertEquals("Invalid security type: bogus. Supported values are: none, oauth2",
                validateError(raw("iceberg.rest.security.type", "bogus",
                        "iceberg.rest.role_arn", "arn", "iceberg.rest.access-key-id", "ak")));
    }

    @Test
    public void fireOrderEagerRoleArnBeforeDeferredRequireTogether() {
        // WHY: role_arn (rule 5) throws eagerly during buildRules(), before the requireTogether (rule 10)
        // ParamRule runs at validate(). So with BOTH role_arn set and AK-only set, role_arn wins.
        // MUTATION: if requireTogether were eager (or role_arn deferred), the AK/SK message would surface.
        Assertions.assertEquals("iceberg.rest.role_arn is not supported for Iceberg REST catalog. "
                        + "Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, "
                        + "or iceberg.rest.credentials_provider_type instead",
                validateError(raw("iceberg.rest.role_arn", "arn", "iceberg.rest.access-key-id", "ak")));
    }
}

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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.datasource.storage.CloudObjectStoreAdapter;
import org.apache.doris.nereids.parser.NereidsParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ShowCreateStorageVaultCommandTest {
    @Test
    public void testNativeGcpIdentityRoundTrip() {
        for (Cloud.GcpCredentialPB.CredentialProviderType providerType : List.of(
                Cloud.GcpCredentialPB.CredentialProviderType.DEFAULT,
                Cloud.GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE)) {
            for (String account : List.of("", "target@project.iam.gserviceaccount.com")) {
                Cloud.GcpCredentialPB.Builder credential = Cloud.GcpCredentialPB.newBuilder()
                        .setCredentialProviderType(providerType);
                if (!account.isEmpty()) {
                    credential.setImpersonationServiceAccount(account);
                }
                Cloud.ObjectStoreInfoPB.Builder original = objectInfo(Cloud.ObjectStoreInfoPB.Provider.GCP);
                original.getCredentialBuilder().setGcpCredential(credential);

                Map<String, String> properties = shownProperties(original.build());
                Assertions.assertEquals(providerType.name(), properties.get("gs.credential_provider_type"));
                Assertions.assertEquals(account, properties.getOrDefault("gs.impersonation_service_account", ""));
                Assertions.assertFalse(properties.containsKey("s3.access_key"));
                Assertions.assertFalse(properties.containsKey("s3.secret_key"));
                Assertions.assertEquals(original.build(), CloudObjectStoreAdapter.getObjStoreInfoPB(properties).build(),
                        "SHOW CREATE must preserve the native identity and connection properties");
            }
        }
    }

    @Test
    public void testLegacyKeysRemainMasked() {
        for (Cloud.ObjectStoreInfoPB.Provider provider : List.of(
                Cloud.ObjectStoreInfoPB.Provider.GCP, Cloud.ObjectStoreInfoPB.Provider.S3)) {
            Map<String, String> properties = shownProperties(objectInfo(provider)
                    .setAk("access-key").setSk("secret-key").build());
            Assertions.assertEquals(DatasourcePrintableMap.PASSWORD_MASK, properties.get("s3.access_key"));
            Assertions.assertEquals(DatasourcePrintableMap.PASSWORD_MASK, properties.get("s3.secret_key"));
            Assertions.assertFalse(properties.containsKey("gs.credential_provider_type"));
            Assertions.assertFalse(properties.containsKey("gs.impersonation_service_account"));
        }
    }

    private Cloud.ObjectStoreInfoPB.Builder objectInfo(Cloud.ObjectStoreInfoPB.Provider provider) {
        return Cloud.ObjectStoreInfoPB.newBuilder()
                .setProvider(provider)
                .setEndpoint("storage.googleapis.com")
                .setExternalEndpoint("storage.googleapis.com")
                .setRegion("us-central1")
                .setBucket("bucket")
                .setPrefix("vault")
                .setUsePathStyle(true);
    }

    private Map<String, String> shownProperties(Cloud.ObjectStoreInfoPB info) {
        ShowCreateStorageVaultCommand command = new ShowCreateStorageVaultCommand("gcp_vault");
        String ddl = Deencapsulation.invoke(command, "getObjectCreateStmt", info);
        CreateStorageVaultCommand parsed = (CreateStorageVaultCommand) new NereidsParser().parseSingle(ddl);
        Assertions.assertEquals("gcp_vault", parsed.getVaultName());
        return parsed.getProperties();
    }
}

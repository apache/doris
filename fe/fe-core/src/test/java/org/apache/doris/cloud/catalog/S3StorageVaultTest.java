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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.filesystem.auth.GcpCredential;
import org.apache.doris.nereids.trees.plans.commands.CreateStorageVaultCommand;
import org.apache.doris.system.SystemInfoService;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class S3StorageVaultTest {
    private static final String ROLE = "arn:aws:iam::123456789012:role/test-role";
    private static final String ACCOUNT = "target@my-project.iam.gserviceaccount.com";

    @Mocked
    private Env env;

    // Capture the actual CREATE/ALTER RPC, then stop before cache updates and backend synchronization.
    private Cloud.ObjectStoreInfoPB captureRequest(Map<String, String> auth, boolean isAlter) throws Exception {
        AtomicReference<Cloud.AlterObjStoreInfoRequest> captured = new AtomicReference<>();
        new MockUp<MetaServiceProxy>() {
            @Mock
            public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request) {
                captured.set(request);
                return Cloud.AlterObjStoreInfoResponse.newBuilder().setStatus(
                        Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                .setMsg("captured vault request")).build();
            }
        };
        String oldEndpoint = Config.meta_service_endpoint;
        String oldCloudId = Config.cloud_unique_id;
        Config.meta_service_endpoint = "127.0.0.1:20121";
        Config.cloud_unique_id = "vault-default-test";
        try {
            StorageVaultMgr mgr = new StorageVaultMgr(new SystemInfoService());
            if (isAlter) {
                Assertions.assertThrows(DdlException.class,
                        () -> mgr.alterStorageVault(StorageVaultType.S3, new HashMap<>(auth), "iam_vault"));
            } else {
                Map<String, String> properties = new HashMap<>(auth);
                boolean isGcp = "GCP".equals(auth.get("provider"))
                        || auth.keySet().stream().anyMatch(key -> key.toLowerCase().startsWith("gs."));
                properties.put("type", "S3");
                properties.put("provider", isGcp ? "GCP" : "S3");
                properties.put(S3ResourceCompat.ENDPOINT, isGcp ? "storage.googleapis.com" : "s3.us-east-1.amazonaws.com");
                properties.put(S3ResourceCompat.REGION, "us-east-1");
                properties.put(S3ResourceCompat.BUCKET, "test-bucket");
                properties.put(S3ResourceCompat.ROOT_PATH, "test-root");
                properties.put(S3ResourceCompat.VALIDITY_CHECK, "false");
                CreateStorageVaultCommand command = new CreateStorageVaultCommand(false, "iam_vault", properties);
                command.setStorageVaultType(StorageVaultType.S3);
                StorageVault vault = StorageVault.fromCommand(command);
                Assertions.assertThrows(DdlException.class, () -> mgr.createS3Vault(vault));
            }
            Assertions.assertNotNull(captured.get(), "The request must reach the MS RPC boundary");
            Assertions.assertEquals(isAlter ? Cloud.AlterObjStoreInfoRequest.Operation.ALTER_S3_VAULT
                    : Cloud.AlterObjStoreInfoRequest.Operation.ADD_S3_VAULT, captured.get().getOp());
            if (isAlter) {
                Assertions.assertFalse(captured.get().getVault().getObjInfo().hasProvider(),
                        "ALTER must not send an inferred immutable provider");
            }
            return captured.get().getVault().getObjInfo();
        } finally {
            Config.meta_service_endpoint = oldEndpoint;
            Config.cloud_unique_id = oldCloudId;
        }
    }

    @Test
    public void testAwsCreatePreservesDefaultAndExplicitProvider() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(S3ResourceCompat.ROLE_ARN, ROLE);
        Cloud.ObjectStoreInfoPB obj = captureRequest(properties, false);
        Assertions.assertTrue(obj.hasCredProviderType());
        Assertions.assertEquals(Cloud.CredProviderTypePB.INSTANCE_PROFILE, obj.getCredProviderType());
        for (String key : new String[] {S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE,
                S3ResourceCompat.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> explicit = new HashMap<>(properties);
            explicit.put(key, "CONTAINER");
            obj = captureRequest(explicit, false);
            Assertions.assertTrue(obj.hasCredProviderType());
            Assertions.assertEquals(Cloud.CredProviderTypePB.CONTAINER, obj.getCredProviderType());
        }
    }

    @Test
    public void testAwsAlterPreservesOmissionAndExplicitClears() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(S3ResourceCompat.ROLE_ARN, ROLE);
        Assertions.assertFalse(captureRequest(properties, true).hasCredProviderType());
        properties.put(S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE, "DEFAULT");
        Cloud.ObjectStoreInfoPB obj = captureRequest(properties, true);
        Assertions.assertTrue(obj.hasCredProviderType());
        Assertions.assertEquals(Cloud.CredProviderTypePB.DEFAULT, obj.getCredProviderType());
        properties.clear();
        properties.put(S3ResourceCompat.ROLE_ARN, "");
        properties.put(S3ResourceCompat.EXTERNAL_ID, "");
        obj = captureRequest(properties, true);
        Assertions.assertTrue(obj.hasRoleArn());
        Assertions.assertEquals("", obj.getRoleArn());
        Assertions.assertTrue(obj.hasExternalId());
        Assertions.assertEquals("", obj.getExternalId());
        Assertions.assertFalse(obj.hasCredProviderType());
    }

    @Test
    public void testGcpCreateDefaultsAndExplicitProvider() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, ACCOUNT);
        Cloud.GcpCredentialPB credential = captureRequest(properties, false).getCredential().getGcpCredential();
        Assertions.assertTrue(credential.hasCredentialProviderType());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.DEFAULT,
                credential.getCredentialProviderType());
        Assertions.assertEquals(ACCOUNT, credential.getImpersonationServiceAccount());
        properties.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        credential = captureRequest(properties, false).getCredential().getGcpCredential();
        Assertions.assertTrue(credential.hasCredentialProviderType());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE,
                credential.getCredentialProviderType());
    }

    @Test
    public void testGcpEmptyTargetDefaultsToDefault() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, "");
        Cloud.GcpCredentialPB credential = captureRequest(properties, false).getCredential().getGcpCredential();
        Assertions.assertTrue(credential.hasCredentialProviderType());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.DEFAULT,
                credential.getCredentialProviderType());
        Assertions.assertFalse(credential.hasImpersonationServiceAccount());

        credential = captureRequest(properties, true).getCredential().getGcpCredential();
        Assertions.assertFalse(credential.hasCredentialProviderType());
        Assertions.assertTrue(credential.hasImpersonationServiceAccount());
        Assertions.assertEquals("", credential.getImpersonationServiceAccount());
    }

    @Test
    public void testGcpAlterPreservesOmittedFields() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, ACCOUNT);
        Cloud.GcpCredentialPB credential = captureRequest(properties, true).getCredential().getGcpCredential();
        Assertions.assertFalse(credential.hasCredentialProviderType());
        Assertions.assertTrue(credential.hasImpersonationServiceAccount());
        Assertions.assertEquals(ACCOUNT, credential.getImpersonationServiceAccount());
        properties.clear();
        properties.put(GcpCredential.CREDENTIAL_PROVIDER_TYPE, "COMPUTE_ENGINE");
        credential = captureRequest(properties, true).getCredential().getGcpCredential();
        Assertions.assertTrue(credential.hasCredentialProviderType());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE,
                credential.getCredentialProviderType());
        Assertions.assertFalse(credential.hasImpersonationServiceAccount());
        properties.put(GcpCredential.IMPERSONATION_SERVICE_ACCOUNT, ACCOUNT);
        credential = captureRequest(properties, true).getCredential().getGcpCredential();
        Assertions.assertTrue(credential.hasCredentialProviderType());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.COMPUTE_ENGINE,
                credential.getCredentialProviderType());
        Assertions.assertTrue(credential.hasImpersonationServiceAccount());
        Assertions.assertEquals(ACCOUNT, credential.getImpersonationServiceAccount());
        properties.clear();
        properties.put(S3ResourceCompat.USE_PATH_STYLE, "true");
        Cloud.ObjectStoreInfoPB obj = captureRequest(properties, true);
        Assertions.assertFalse(obj.hasCredential());
        Assertions.assertFalse(obj.hasCredProviderType());
    }

    @Test
    public void testGcpCreateWithoutAuthenticationDefaultsToAdc() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "GCP");
        Cloud.ObjectStoreInfoPB obj = captureRequest(properties, false);
        Assertions.assertTrue(obj.hasCredential());
        Assertions.assertEquals(Cloud.GcpCredentialPB.CredentialProviderType.DEFAULT,
                obj.getCredential().getGcpCredential().getCredentialProviderType());
        Assertions.assertFalse(obj.getCredential().getGcpCredential().hasImpersonationServiceAccount());
    }

    @Test
    public void testGcpAnonymousAliasesAreRejectedBeforeCreateRpc() throws Exception {
        new MockUp<MetaServiceProxy>() {
            @Mock
            public Cloud.AlterObjStoreInfoResponse alterStorageVault(Cloud.AlterObjStoreInfoRequest request) {
                throw new AssertionError("Anonymous GCS vaults must be rejected before the MS RPC");
            }
        };
        StorageVaultMgr mgr = new StorageVaultMgr(new SystemInfoService());
        for (String key : new String[] {GcpCredential.CREDENTIAL_PROVIDER_TYPE,
                S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE, S3ResourceCompat.Env.CREDENTIALS_PROVIDER_TYPE}) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "S3");
            properties.put("provider", "GCP");
            properties.put(S3ResourceCompat.ENDPOINT, "storage.googleapis.com");
            properties.put(S3ResourceCompat.REGION, "us-east1");
            properties.put(S3ResourceCompat.BUCKET, "test-bucket");
            properties.put(S3ResourceCompat.ROOT_PATH, "test-root");
            properties.put(S3ResourceCompat.VALIDITY_CHECK, "false");
            properties.put(key, " anonymous ");
            CreateStorageVaultCommand command = new CreateStorageVaultCommand(false, "anonymous_gcp", properties);
            command.setStorageVaultType(StorageVaultType.S3);
            StorageVault vault = StorageVault.fromCommand(command);
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> mgr.createS3Vault(vault), key);
            Assertions.assertTrue(error.getMessage().contains("storage vaults"), key);
        }
    }

    @Test
    public void testGcpStorageOnlyAlterDoesNotInjectAdc() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "storage.googleapis.com");
        // Inspect the builder directly: ALTER's public API may reject immutable storage fields.
        java.lang.reflect.Method method = StorageVaultMgr.class.getDeclaredMethod(
                "buildAlterS3VaultRequest", Map.class, String.class);
        method.setAccessible(true);
        StorageVaultMgr mgr = new StorageVaultMgr(new SystemInfoService());
        Cloud.StorageVaultPB.Builder request = (Cloud.StorageVaultPB.Builder) method.invoke(mgr, properties, "gcp");
        Assertions.assertFalse(request.getObjInfo().hasCredential());
        Assertions.assertFalse(request.getObjInfo().hasProvider());
    }

}

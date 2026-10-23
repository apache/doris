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
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.nereids.trees.plans.commands.CreateStorageVaultCommand;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class S3StorageVaultTest {
    private static final String ROLE = "arn:aws:iam::123456789012:role/test-role";

    // Capture the actual CREATE/ALTER RPC, then stop before cache updates and backend synchronization.
    private Cloud.ObjectStoreInfoPB captureRequest(Map<String, String> auth, boolean isAlter) throws Exception {
        AtomicReference<Cloud.AlterObjStoreInfoRequest> captured = new AtomicReference<>();
        Env env = Mockito.mock(Env.class);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class);
                MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(proxy.alterStorageVault(Mockito.any(Cloud.AlterObjStoreInfoRequest.class)))
                    .thenAnswer(invocation -> {
                        captured.set(invocation.getArgument(0));
                        return Cloud.AlterObjStoreInfoResponse.newBuilder().setStatus(
                                Cloud.MetaServiceResponseStatus.newBuilder()
                                        .setCode(Cloud.MetaServiceCode.INVALID_ARGUMENT)
                                        .setMsg("captured vault request")).build();
                    });
            StorageVaultMgr mgr = new StorageVaultMgr(new SystemInfoService());
            if (isAlter) {
                Assertions.assertThrows(DdlException.class,
                        () -> mgr.alterStorageVault(StorageVaultType.S3, new HashMap<>(auth), "iam_vault"));
            } else {
                Map<String, String> properties = new HashMap<>(auth);
                properties.put("type", "S3");
                properties.put("provider", "S3");
                properties.put(S3ResourceCompat.ENDPOINT, "s3.us-east-1.amazonaws.com");
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

}

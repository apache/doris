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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVault.StorageVaultType;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class AlterStorageVaultCommandTest {
    private static final String VAULT_NAME = "test_s3_vault";

    @Test
    public void testRejectPathStyleForS3ExpressVault() throws Exception {
        assertRejected(ImmutableMap.of(S3ResourceCompat.USE_PATH_STYLE, "true"),
                "S3 Express requires use_path_style=false");
    }

    @Test
    public void testRejectAnonymousCredentialsForS3ExpressVault() throws Exception {
        assertRejected(ImmutableMap.of(S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE, " anonymous "),
                "S3 Express does not support anonymous access");
    }

    @Test
    public void testNormalS3VaultIsUnaffected() throws Exception {
        Map<String, String> properties = ImmutableMap.of(
                S3ResourceCompat.USE_PATH_STYLE, "true",
                S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE, "ANONYMOUS");
        StorageVaultMgr storageVaultMgr = mockStorageVaultMgr(false);

        try (MockedStatic<Env> mockedEnv = mockEnv(storageVaultMgr)) {
            AlterStorageVaultCommand command = new AlterStorageVaultCommand(VAULT_NAME, properties);
            Assertions.assertDoesNotThrow(() -> command.run(null, null));
        }

        Mockito.verify(storageVaultMgr).alterStorageVault(StorageVaultType.S3, properties, VAULT_NAME);
    }

    @Test
    public void testValidS3ExpressAlterIsAccepted() throws Exception {
        Map<String, String> properties = ImmutableMap.of(
                S3ResourceCompat.USE_PATH_STYLE, "false",
                S3ResourceCompat.CREDENTIALS_PROVIDER_TYPE, "DEFAULT");
        StorageVaultMgr storageVaultMgr = mockStorageVaultMgr(true);

        try (MockedStatic<Env> mockedEnv = mockEnv(storageVaultMgr)) {
            AlterStorageVaultCommand command = new AlterStorageVaultCommand(VAULT_NAME, properties);
            Assertions.assertDoesNotThrow(() -> command.run(null, null));
        }

        Mockito.verify(storageVaultMgr).alterStorageVault(StorageVaultType.S3, properties, VAULT_NAME);
    }

    private void assertRejected(Map<String, String> properties, String expectedMessage) throws Exception {
        StorageVaultMgr storageVaultMgr = mockStorageVaultMgr(true);
        try (MockedStatic<Env> mockedEnv = mockEnv(storageVaultMgr)) {
            AlterStorageVaultCommand command = new AlterStorageVaultCommand(VAULT_NAME, properties);
            AnalysisException exception = Assertions.assertThrows(
                    AnalysisException.class, () -> command.run(null, null));
            Assertions.assertEquals(expectedMessage, exception.getDetailMessage());
        }
        Mockito.verify(storageVaultMgr, Mockito.never()).alterStorageVault(
                Mockito.any(), Mockito.anyMap(), Mockito.anyString());
    }

    private StorageVaultMgr mockStorageVaultMgr(boolean s3Express) throws Exception {
        StorageVaultMgr storageVaultMgr = Mockito.mock(StorageVaultMgr.class);
        Mockito.when(storageVaultMgr.getStorageVaultTypeByName(VAULT_NAME)).thenReturn(StorageVaultType.S3);
        Mockito.when(storageVaultMgr.isS3ExpressStorageVault(VAULT_NAME)).thenReturn(s3Express);
        return storageVaultMgr;
    }

    private MockedStatic<Env> mockEnv(StorageVaultMgr storageVaultMgr) {
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getStorageVaultMgr()).thenReturn(storageVaultMgr);
        Mockito.when(accessManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(true);

        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }
}

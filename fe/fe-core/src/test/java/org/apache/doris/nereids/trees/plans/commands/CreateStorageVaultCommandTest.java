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
import org.apache.doris.catalog.StorageVault;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CreateStorageVaultCommandTest extends TestWithFeService {
    private String vaultName;

    @Override
    protected void runBeforeAll() throws Exception {
        vaultName = "hdfs_nereids";
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testValidateNormal() throws Exception {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Config.cloud_unique_id = "not_empty_nereids";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "hdfs")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());
        Assertions.assertEquals(vaultName, command.getVaultName());
        Assertions.assertEquals(StorageVault.StorageVaultType.HDFS, command.getVaultType());

        // testUnsupportedResourceType
        ImmutableMap<String, String> properties1 = ImmutableMap.<String, String>builder()
                .put("type", "hadoop")
                .build();
        CreateStorageVaultCommand command1 = new CreateStorageVaultCommand(true, vaultName, properties1);
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate());
        Config.cloud_unique_id = "";
    }

    @Test
    public void testValidatePathFormat() throws Exception {
        Env env = Env.getCurrentEnv();
        AccessControllerManager accessManager = env.getAccessManager();
        AccessControllerManager spyAcm = Mockito.spy(accessManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        Config.cloud_unique_id = "not_empty_nereids";
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName,
                ImmutableMap.of("type", "hdfs", "path_version", "1", "shard_num", "1024"));
        Assertions.assertDoesNotThrow(() -> command.validate());
        Assertions.assertEquals(1, command.getPathVersion());
        Assertions.assertEquals(1024, command.getNumShard());
        Assertions.assertFalse(command.getProperties().containsKey("path_version"));
        Assertions.assertFalse(command.getProperties().containsKey("shard_num"));
        Assertions.assertEquals("hdfs", command.getProperties().get("type"));

        CreateStorageVaultCommand command0 = new CreateStorageVaultCommand(true, vaultName,
                ImmutableMap.of("type", "hdfs", "path_version", "0"));
        Assertions.assertDoesNotThrow(() -> command0.validate());
        Assertions.assertEquals(0, command0.getPathVersion());
        Assertions.assertEquals(0, command0.getNumShard());

        String[][] invalidPathFormats = {
                {"path_version", "1"},
                {"path_version", "2"},
                {"path_version", "-1"},
                {"path_version", "abc"},
                {"shard_num", "16"},
                {"path_version", "1", "shard_num", "0"},
                {"path_version", "1", "shard_num", "-8"},
                {"path_version", "1", "shard_num", "abc"},
                {"path_version", "0", "shard_num", "16"},
        };
        for (String[] pathFormat : invalidPathFormats) {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder().put("type", "hdfs");
            for (int i = 0; i < pathFormat.length; i += 2) {
                builder.put(pathFormat[i], pathFormat[i + 1]);
            }
            CreateStorageVaultCommand invalid = new CreateStorageVaultCommand(true, vaultName, builder.build());
            Assertions.assertThrows(AnalysisException.class, () -> invalid.validate(),
                    String.join(",", pathFormat));
        }
        Config.cloud_unique_id = "";
    }
}

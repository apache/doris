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
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateStorageVaultCommandTest extends TestWithFeService {
    private String vaultName;

    @Override
    protected void runBeforeAll() throws Exception {
        vaultName = "hdfs";
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testValidateNormal(@Mocked AccessControllerManager accessManager) {
        new Expectations() {
            {
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Config.cloud_unique_id = "not_empty";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "hdfs")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());
        Assertions.assertEquals(vaultName, command.getVaultName());
        Assertions.assertEquals(StorageVault.StorageVaultType.HDFS, command.getVaultType());
        Config.cloud_unique_id = "";
    }

    @Test
    public void testUnsupportedResourceType(@Mocked Env env, @Mocked AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Config.cloud_unique_id = "not_empty";
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "hadoop")
                .build();
        CreateStorageVaultCommand command = new CreateStorageVaultCommand(true, vaultName, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
        Config.cloud_unique_id = "";
    }
}

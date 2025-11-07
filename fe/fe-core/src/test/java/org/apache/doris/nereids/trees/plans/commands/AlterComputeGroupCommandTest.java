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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterComputeGroupCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private CloudSystemInfoService cloudSystemInfoService;
    @Mocked
    private ComputeGroup computeGroup;
    private Database db;

    private void runBefore() throws Exception {
        db = CatalogMocker.mockDb();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNonCloudMode() throws Exception {
        runBefore();
        Config.deploy_mode = "non-cloud";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateAuthFailed() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateEmptyProperties() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", null);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));

        AlterComputeGroupCommand command2 = new AlterComputeGroupCommand("test_group", new HashMap<>());
        Assertions.assertThrows(UserException.class, () -> command2.validate(connectContext));
    }

    @Test
    public void testValidateComputeGroupNotExist() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        new Expectations() {
            {
                env.getCurrentSystemInfo();
                minTimes = 0;
                result = cloudSystemInfoService;

                cloudSystemInfoService.getComputeGroupByName("non_exist_group");
                minTimes = 0;
                result = null;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("non_exist_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateVirtualComputeGroup() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        new Expectations() {
            {
                env.getCurrentSystemInfo();
                minTimes = 0;
                result = cloudSystemInfoService;

                cloudSystemInfoService.getComputeGroupByName("virtual_group");
                minTimes = 0;
                result = computeGroup;

                computeGroup.isVirtual();
                minTimes = 0;
                result = true;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("virtual_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateInvalidProperties() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        new Expectations() {
            {
                env.getCurrentSystemInfo();
                minTimes = 0;
                result = cloudSystemInfoService;

                cloudSystemInfoService.getComputeGroupByName("test_group");
                minTimes = 0;
                result = computeGroup;

                computeGroup.isVirtual();
                minTimes = 0;
                result = false;

                computeGroup.checkProperties((Map<String, String>) any);
                minTimes = 0;
                result = new DdlException("Invalid property");
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("invalid_property", "invalid_value");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateSuccess() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        new Expectations() {
            {
                env.getCurrentSystemInfo();
                minTimes = 0;
                result = cloudSystemInfoService;

                cloudSystemInfoService.getComputeGroupByName("test_group");
                minTimes = 0;
                result = computeGroup;

                computeGroup.isVirtual();
                minTimes = 0;
                result = false;
            }
        };

        new Expectations() {
            {
                computeGroup.checkProperties((Map<String, String>) any);
                minTimes = 0;

                computeGroup.modifyProperties((Map<String, String>) any);
                minTimes = 0;
            }
        };

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        properties.put("balance_warm_up_task_timeout", "300");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}

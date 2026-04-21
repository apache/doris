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
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AlterComputeGroupCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        accessControllerManager = spyAcm;
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
        Mockito.doReturn(false).when(accessControllerManager).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));

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

        CloudSystemInfoService mockCloudSIS = Mockito.mock(CloudSystemInfoService.class);
        Mockito.doReturn(null).when(mockCloudSIS).getComputeGroupByName("non_exist_group");
        Deencapsulation.setField(env, "systemInfo", mockCloudSIS);

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("non_exist_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateVirtualComputeGroup() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        CloudSystemInfoService mockCloudSIS = Mockito.mock(CloudSystemInfoService.class);
        ComputeGroup mockComputeGroup = Mockito.mock(ComputeGroup.class);
        Mockito.doReturn(mockComputeGroup).when(mockCloudSIS).getComputeGroupByName("virtual_group");
        Mockito.doReturn(true).when(mockComputeGroup).isVirtual();
        Deencapsulation.setField(env, "systemInfo", mockCloudSIS);

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("virtual_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateInvalidProperties() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        CloudSystemInfoService mockCloudSIS = Mockito.mock(CloudSystemInfoService.class);
        ComputeGroup mockComputeGroup = Mockito.mock(ComputeGroup.class);
        Mockito.doReturn(mockComputeGroup).when(mockCloudSIS).getComputeGroupByName("test_group");
        Mockito.doReturn(false).when(mockComputeGroup).isVirtual();
        Mockito.doThrow(new DdlException("Invalid property")).when(mockComputeGroup)
                .checkProperties(Mockito.anyMap());
        Deencapsulation.setField(env, "systemInfo", mockCloudSIS);

        Map<String, String> properties = Maps.newHashMap();
        properties.put("invalid_property", "invalid_value");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext));
    }

    @Test
    public void testValidateSuccess() throws Exception {
        runBefore();
        Config.deploy_mode = "cloud";

        CloudSystemInfoService mockCloudSIS = Mockito.mock(CloudSystemInfoService.class);
        ComputeGroup mockComputeGroup = Mockito.mock(ComputeGroup.class);
        Mockito.doReturn(mockComputeGroup).when(mockCloudSIS).getComputeGroupByName("test_group");
        Mockito.doReturn(false).when(mockComputeGroup).isVirtual();
        Mockito.doNothing().when(mockComputeGroup).checkProperties(Mockito.anyMap());
        Mockito.doNothing().when(mockComputeGroup).modifyProperties(Mockito.anyMap());
        Deencapsulation.setField(env, "systemInfo", mockCloudSIS);

        Map<String, String> properties = Maps.newHashMap();
        properties.put("balance_type", "async_warmup");
        properties.put("balance_warm_up_task_timeout", "300");
        AlterComputeGroupCommand command = new AlterComputeGroupCommand("test_group", properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}

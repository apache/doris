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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.analysis.UserIdentity;

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RangerHiveAccessControllerTest {
    @Test
    public void testRangerAccessTypeMapping() {
        Assert.assertEquals("select", RangerHiveAccessController.toRangerAccessType(HiveAccessType.SELECT));
        Assert.assertEquals("update", RangerHiveAccessController.toRangerAccessType(HiveAccessType.UPDATE));
        Assert.assertEquals(RangerPolicyEngine.ANY_ACCESS,
                RangerHiveAccessController.toRangerAccessType(HiveAccessType.USE));
    }

    @Test
    public void testPolicyRequestsUseLowerCaseSelect() throws Exception {
        RangerHiveAccessController controller = Mockito.mock(
                RangerHiveAccessController.class, Mockito.CALLS_REAL_METHODS);
        Field lifecycleLock = RangerHiveAccessController.class.getDeclaredField("lifecycleLock");
        lifecycleLock.setAccessible(true);
        lifecycleLock.set(controller, new ReentrantReadWriteLock());
        RangerBasePlugin plugin = Mockito.mock(RangerBasePlugin.class);
        UserIdentity currentUser = UserIdentity.createAnalyzedUserIdentWithIp("user", "%");
        RangerAccessRequestImpl rowFilterRequest = new RangerAccessRequestImpl();
        RangerAccessRequestImpl dataMaskRequest = new RangerAccessRequestImpl();

        Mockito.doReturn(rowFilterRequest, dataMaskRequest).when(controller).createRequest(currentUser);
        Mockito.doReturn(plugin).when(controller).getPlugin();

        controller.evalRowFilterPolicies(currentUser, "catalog", "database", "table");
        controller.evalDataMaskPolicy(currentUser, "catalog", "database", "table", "column");

        Assert.assertEquals("select", rowFilterRequest.getAccessType());
        Assert.assertEquals("select", dataMaskRequest.getAccessType());
    }
}

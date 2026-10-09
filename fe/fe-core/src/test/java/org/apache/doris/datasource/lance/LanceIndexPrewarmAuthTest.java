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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class LanceIndexPrewarmAuthTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager access;
    @Mocked
    private ConnectContext context;
    @Mocked
    private ComputeGroupMgr groups;

    private void environment(boolean admin, boolean select) {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = access;
                access.checkGlobalPriv(context, PrivPredicate.ADMIN);
                result = admin;
                minTimes = 0;
                access.checkTblPriv(context, withInstanceOf(TableName.class), PrivPredicate.SELECT);
                result = select;
                minTimes = 0;
            }
        };
    }

    @Test
    public void checksAdminAndSelectBeforeExternalCatalogLookup() throws Exception {
        environment(false, true);
        TableNameInfo table = new TableNameInfo("lake", "db", "items");
        Assert.assertTrue(Assert.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.run(context, null, table, Collections.emptyList(), () -> false))
                .getMessage().contains("ADMIN"));
        new Verifications() {
            {
                env.getCatalogMgr();
                times = 0;
            }
        };
    }

    @Test
    public void requiresTableSelectEvenWhenAdminCheckPasses() {
        environment(true, false);
        Assert.assertTrue(Assert.assertThrows(UserException.class,
                () -> LanceIndexPrewarm.checkPrivileges(context, new TableNameInfo("lake", "db", "items")))
                .getMessage().contains("SELECT"));
    }

    @Test
    public void usesSessionResourceGroupInNonCloudMode() throws Exception {
        String old = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "";
            ComputeGroup group = new ComputeGroup("selected", "selected", null);
            new Expectations() {
                {
                    context.getComputeGroup();
                    result = group;
                }
            };
            Assert.assertSame(group, LanceIndexPrewarm.resolveComputeGroup(context));
        } finally {
            Config.cloud_unique_id = old;
        }
    }

    @Test
    public void checksUsageForSessionCloudGroup() throws Exception {
        String old = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "test-cloud";
            ComputeGroup group = new ComputeGroup("selected", "selected", null);
            new Expectations() {
                {
                    Env.getCurrentEnv();
                    result = env;
                    env.getAccessManager();
                    result = access;
                    context.getCloudCluster();
                    result = "session-group";
                    access.checkCloudPriv((UserIdentity) any, "session-group", PrivPredicate.USAGE,
                            ResourceTypeEnum.CLUSTER);
                    result = true;
                    env.getComputeGroupMgr();
                    result = groups;
                    groups.getComputeGroupByName(anyString);
                    result = group;
                }
            };
            Assert.assertSame(group, LanceIndexPrewarm.resolveComputeGroup(context));
            new Expectations() {
                {
                    access.checkCloudPriv((UserIdentity) any, "session-group", PrivPredicate.USAGE,
                            ResourceTypeEnum.CLUSTER);
                    result = false;
                }
            };
            Assert.assertThrows(UserException.class, () -> LanceIndexPrewarm.resolveComputeGroup(context));
        } finally {
            Config.cloud_unique_id = old;
        }
    }
}

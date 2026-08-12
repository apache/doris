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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class ShowComputeGroupTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    public void testShowComputeGroupsInCloudMode() throws Exception {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "cloud_unique_id";
        ShowClustersCommand command = new ShowClustersCommand(true);
        ShowResultSetMetaData metaData = command.getMetaData();
        Assertions.assertNotNull(metaData);
        List<String> columnNames = metaData.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(7, columnNames.size());
        Assertions.assertEquals("Name", columnNames.get(0));
        Assertions.assertEquals("IsCurrent", columnNames.get(1));
        Assertions.assertEquals("Users", columnNames.get(2));
        Assertions.assertEquals("BackendNum", columnNames.get(3));
        Assertions.assertEquals("SubComputeGroups", columnNames.get(4));
        Assertions.assertEquals("Policy", columnNames.get(5));
        Assertions.assertEquals("Properties", columnNames.get(6));
    }

    @Test
    public void testShowComputeGroupsInNonCloudMode() throws Exception {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        ShowClustersCommand command = new ShowClustersCommand(true);
        List<String> columnNames = command.getMetaData().getColumns().stream()
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(Lists.newArrayList("Name", "BackendNum"), columnNames);
        // resource group of the backends registered by the test frame
        List<List<String>> rows = command.doRun(connectContext, null).getResultRows();
        Assertions.assertFalse(rows.isEmpty());
        rows.forEach(row -> Assertions.assertEquals(2, row.size()));

        // a user restricted by resource_tags.location only sees the resource groups it can use,
        // this is the compute group bound to the session when the user logs in.
        executeSql("CREATE USER show_cg_user IDENTIFIED BY '12345'");
        try {
            String beTag = rows.get(0).get(0);
            executeSql("SET PROPERTY FOR 'show_cg_user' 'resource_tags.location' = '" + beTag + "'");
            connectContext.setComputeGroup(Env.getCurrentEnv().getAuth().getComputeGroup("show_cg_user"));
            Assertions.assertEquals(rows.subList(0, 1), command.doRun(connectContext, null).getResultRows());

            executeSql("SET PROPERTY FOR 'show_cg_user' 'resource_tags.location' = 'no_such_resource_group'");
            connectContext.setComputeGroup(Env.getCurrentEnv().getAuth().getComputeGroup("show_cg_user"));
            Assertions.assertTrue(command.doRun(connectContext, null).getResultRows().isEmpty());
        } finally {
            connectContext.setComputeGroup(null);
        }
    }

    @Test
    public void testShowClustersInCloudMode() throws Exception {
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "cloud_unique_id";
        ShowClustersCommand command = new ShowClustersCommand(false);
        ShowResultSetMetaData metaData = command.getMetaData();
        Assertions.assertNotNull(metaData);
        List<String> columnNames = metaData.getColumns().stream()
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(7, columnNames.size());
        Assertions.assertEquals("cluster", columnNames.get(0));
        Assertions.assertEquals("is_current", columnNames.get(1));
        Assertions.assertEquals("users", columnNames.get(2));
        Assertions.assertEquals("backend_num", columnNames.get(3));
        Assertions.assertEquals("sub_clusters", columnNames.get(4));
        Assertions.assertEquals("policy", columnNames.get(5));
        Assertions.assertEquals("properties", columnNames.get(6));
    }

    @Test
    public void testShowClustersInNonCloudMode() throws Exception {
        Config.deploy_mode = "";
        Config.cloud_unique_id = "";
        ShowClustersCommand command = new ShowClustersCommand(false);
        List<String> columnNames = command.getMetaData().getColumns().stream()
                .map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(Lists.newArrayList("cluster", "backend_num"), columnNames);
    }
}

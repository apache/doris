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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.utframe.TestWithFeService;

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
        Config.deploy_mode = "not-cloud";
        ShowClustersCommand command = new ShowClustersCommand(true);
        Assertions.assertThrows(AnalysisException.class, () -> {
            command.doRun(connectContext, null);
        });
    }

    @Test
    public void testShowClustersInCloudMode() throws Exception {
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
        Config.deploy_mode = "not-cloud";
        ShowClustersCommand command = new ShowClustersCommand(false);
        Assertions.assertThrows(AnalysisException.class, () -> {
            command.doRun(connectContext, null);
        });
    }
}

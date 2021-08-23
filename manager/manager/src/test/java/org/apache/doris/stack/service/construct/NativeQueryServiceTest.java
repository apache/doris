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

package org.apache.doris.stack.service.construct;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.connector.PaloQueryClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
@Slf4j
public class NativeQueryServiceTest {
    @InjectMocks
    private NativeQueryService queryService;

    @Mock
    private PaloQueryClient paloQueryClient;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private DatabuildComponent databuildComponent;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     *The test executes SQL query through DB ID information
     */
    @Test
    public void executeSqlByDbIdTest() {
        log.debug("execute sql by dbId test.");
        int nsId = 0;
        int clusterId = 1;
        int dbId = 2;
        int userId = 3;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        // Execute the query of metabase, dbid is less than 1
        try {
            String sql = "select * from collations";
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            queryService.executeSql(nsId, -1, sql, userId);
        } catch (Exception e) {
            log.error("execute sql by dbId test error.");
            e.printStackTrace();
        }

        // Execute queries on other databases, dbid greater than 1
        // mock db
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setName("db");
        databaseEntity.setId(dbId);
        databaseEntity.setClusterId(clusterId);
        try {
            String sql = "select * from table";
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            queryService.executeSql(nsId, dbId, sql, userId);
        } catch (Exception e) {
            log.error("execute sql by dbId test error.");
            e.printStackTrace();
        }
    }

    /**
     * Test query SQL request
     */
    @Test
    public void executeSqlByTest() {
        log.debug("execute sql test.");
        int clusterId = 1;
        int userId = 2;
        String dbName = "db";
        String sql = "select * from table";

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            queryService.executeSql(sql, dbName, userId);
        } catch (Exception e) {
            log.error("execute sql by dbId test error.");
            e.printStackTrace();
        }
    }

    // mock cluster
    private ClusterInfoEntity mockClusterInfo(int clusterId) {
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(clusterId);
        clusterInfo.setName("doris1");
        clusterInfo.setAddress("10.23.32.32");
        clusterInfo.setHttpPort(8030);
        clusterInfo.setQueryPort(8031);
        clusterInfo.setUser("admin");
        clusterInfo.setPasswd("1234");
        clusterInfo.setTimezone("Asia/Shanghai");
        return clusterInfo;
    }
}

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

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.request.construct.DbCreateReq;
import org.apache.doris.stack.model.request.construct.TableCreateReq;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.component.ManagerMetaSyncComponent;
import org.apache.doris.stack.connector.PaloMetaInfoClient;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.driver.DorisDataBuildDriver;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class DataManageServiceTest {

    @InjectMocks
    private DataManageService manageService;

    @Mock
    private ClusterInfoRepository clusterInfoRepository;

    @Mock
    private NativeQueryService nativeQueryService;

    @Mock
    private DorisDataBuildDriver driver;

    @Mock
    private ManagerMetaSyncComponent syncComponent;

    @Mock
    private PaloMetaInfoClient metaInfoClient;

    @Mock
    private CoreUserRepository userRepository;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private DatabuildComponent databuildComponent;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * create database test
     */
    @Test
    public void createDatabseTest() {
        log.debug("Create database test");
        int nsId = 0;
        int userId = 1;
        int clusterId = 2;
        DbCreateReq createReq = new DbCreateReq();

        // request exception
        try {
            manageService.createDatabse(nsId, createReq, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }
        createReq.setName("db");

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // mock sql
        String sql = "CREATE DATABASE db";

        // mock user
        CoreUserEntity userEntity = mockUser(userId);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(driver.createDb(createReq.getName())).thenReturn(sql);
            manageService.createDatabse(nsId, createReq, userId);
        } catch (Exception e) {
            log.error("create database test error.");
            e.printStackTrace();
        }

        // Metadata storage exception, regression creation
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(driver.createDb(createReq.getName())).thenReturn(sql);
            DataDescription description = new DataDescription(createReq.getDescribe(), userEntity.getFirstName());
            when(syncComponent.addDatabase(createReq.getName(), JSON.toJSONString(description),
                    nsId, clusterId)).thenThrow(new Exception("save metabase error"));
            manageService.createDatabse(nsId, createReq, userId);
        } catch (Exception e) {
            log.debug("create database test error {}.", e.getMessage());
        }
    }

    /**
     * delete database test
     */
    @Test
    public void deleteDatabseTest() {
        log.debug("delete database test.");
        int nsId = 0;
        int dbId = 1;
        int userId = 2;
        int clusterId = 3;

        // delete exception
        try {
            manageService.deleteDatabse(nsId, -1, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), NoPermissionException.MESSAGE);
        }

        // delete
        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        // mock db
        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            manageService.deleteDatabse(nsId, dbId, userId);
        } catch (Exception e) {
            log.error("Get database info error {}.", e.getMessage());
        }
    }

    /**
     * create table test
     */
    @Test
    public void createTableTest() {
        log.debug("create table test.");
        int nsId = 0;
        int dbId = 1;
        int userId = 2;
        int clusterId = 3;

        // mock request
        TableCreateReq createTableInfo = new TableCreateReq();
        createTableInfo.setName("table");
        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        //
        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");
        // mock user
        CoreUserEntity userEntity = mockUser(userId);
        String sql = "create table";

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            when(driver.createTable(createTableInfo)).thenReturn(sql);
            // test get create table sql
            String sqlResult = manageService.createTableSql(nsId, dbId, createTableInfo);
            Assert.assertEquals(sqlResult, sql);
            // test create table
            manageService.createTable(nsId, dbId, createTableInfo, userId);
        } catch (Exception e) {
            log.error("create table test error.");
            e.printStackTrace();
        }

        // save metadata exception
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            DataDescription description = new DataDescription(createTableInfo.getDescribe(), userEntity.getFirstName());
            when(syncComponent.addTable(dbId, "table", JSON.toJSONString(description),
                    null)).thenThrow(new Exception("Save metadata error."));
            manageService.createTable(nsId, dbId, createTableInfo, userId);
        } catch (Exception e) {
            log.debug("create table test error {}.", e.getMessage());
        }
    }

    @Test
    public void crateTableBySqlTest() {
        log.debug("create table by sql test.");
        int nsId = 0;
        int dbId = 1;
        int userId = 2;
        int clusterId = 3;

        // sql exception
        try {
            manageService.crateTableBySql(nsId, dbId, null, userId);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), RequestFieldNullException.MESSAGE);
        }

        // mock sql
        String sql = "CREATE TABLE table(\n"
                + "field1 DATE NOT NULL DEFAULT \"2013-05-06\" COMMENT \"test field\",field2 CHAR(0) NOT NULL,"
                + "field3 VARCHAR(0),field4 DECIMAL(0,0) NOT NULL)\n"
                + "ENGINE=olap\n"
                + ";";
        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);
        // mock ddatabase
        ManagerDatabaseEntity databaseEntity = mockDatabase(dbId, clusterId, "db");
        // mock user
        CoreUserEntity userEntity = mockUser(userId);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(databaseEntity);
            manageService.crateTableBySql(nsId, dbId, sql, userId);
        } catch (Exception e) {
            log.error("create table test error.");
            e.printStackTrace();
        }
    }

    private ManagerDatabaseEntity mockDatabase(int dbId, int clusterId, String dbName) {
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setClusterId(clusterId);
        databaseEntity.setId(dbId);
        databaseEntity.setName(dbName);
        databaseEntity.setDescription("test");
        return databaseEntity;
    }

    private CoreUserEntity mockUser(int userId) {
        CoreUserEntity userEntity = new CoreUserEntity();
        userEntity.setId(userId);
        userEntity.setFirstName("user");
        userEntity.setSuperuser(true);
        when(userRepository.findById(userId)).thenReturn(Optional.of(userEntity));
        return userEntity;
    }

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

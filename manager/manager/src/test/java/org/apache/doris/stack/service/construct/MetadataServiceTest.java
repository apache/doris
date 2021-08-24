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
import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.palo.TableSchemaInfo;
import org.apache.doris.stack.model.response.construct.DatabaseResp;
import org.apache.doris.stack.model.response.construct.TableResp;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.component.ManagerMetaSyncComponent;
import org.apache.doris.stack.connector.PaloMetaInfoClient;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.ManagerDatabaseRepository;
import org.apache.doris.stack.dao.ManagerFieldRepository;
import org.apache.doris.stack.dao.ManagerTableRepository;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.entity.ManagerFieldEntity;
import org.apache.doris.stack.entity.ManagerTableEntity;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class MetadataServiceTest {
    @InjectMocks
    private MetadataService metadataService;

    @Mock
    private ClusterInfoRepository clusterInfoRepository;

    @Mock
    private ManagerMetaSyncComponent syncComponent;

    @Mock
    private ManagerDatabaseRepository databaseRepository;

    @Mock
    private ManagerTableRepository tableRepository;

    @Mock
    private ManagerFieldRepository fieldRepository;

    @Mock
    private PaloMetaInfoClient metaInfoClient;

    @Mock
    private ClusterUserComponent clusterUserComponent;

    @Mock
    private DatabuildComponent databuildComponent;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void getDatabaseListByNsTest() {
        log.debug("get database list by ns test");
        int clusterId = 1;
        int userId = 2;
        int ns = 3;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // mock database
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setClusterId(clusterId);
        databaseEntity.setId(4);
        databaseEntity.setName("db");
        List<ManagerDatabaseEntity> databaseEntities = Lists.newArrayList(databaseEntity);
        when(databaseRepository.getByClusterId(clusterId)).thenReturn(databaseEntities);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            List<Map<String, Object>> result = metadataService.getDatabaseListByNs(ns, userId);
            Assert.assertEquals(result.size(), 2);
        } catch (Exception e) {
            log.error("Get database list error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void getDatabaseInfoTest() {
        log.debug("Get database info test");
        int clusterId = 1;
        int userId = 2;
        int dbId = 3;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // get Doris information_schema
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            DatabaseResp result = metadataService.getDatabaseInfo(-1, userId);
            Assert.assertEquals(result.getName(), ConstantDef.MYSQL_DEFAULT_SCHEMA);
        } catch (Exception e) {
            log.error("Get database info error {}.", e.getMessage());
            e.printStackTrace();
        }

        // get other db
        // mock db,description empty
        String dbName = "db1";
        ManagerDatabaseEntity database = new ManagerDatabaseEntity();
        database.setName(dbName);
        database.setId(dbId);
        database.setClusterId(clusterId);
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(database);
            DatabaseResp result = metadataService.getDatabaseInfo(dbId, userId);
            Assert.assertEquals(result.getName(), dbName);
        } catch (Exception e) {
            log.error("Get database info error {}.", e.getMessage());
            e.printStackTrace();
        }

        // mock db,description not empty
        DataDescription description = new DataDescription();
        description.setUserName("user1");
        description.setDescription("test");
        database.setDescription(JSON.toJSONString(description));
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(database);
            DatabaseResp result = metadataService.getDatabaseInfo(dbId, userId);
            Assert.assertEquals(result.getDescribe(), description.getDescription());
            Assert.assertEquals(result.getCreator(), description.getUserName());
        } catch (Exception e) {
            log.error("Get database info error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void getTableListByDbTest() {
        log.debug("Get database table list test");
        int clusterId = 1;
        int userId = 2;
        int dbId = 3;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // get Doris information_schema table
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            List<Map<String, Object>> result = metadataService.getTableListByDb(-1, userId);
            Assert.assertEquals(result.size(), 17);
        } catch (Exception e) {
            log.error("Get database table list error {}.", e.getMessage());
            e.printStackTrace();
        }

        // get other table
        // mock table
        ManagerTableEntity tableEntity = new ManagerTableEntity();
        tableEntity.setId(1);
        tableEntity.setDbId(dbId);
        tableEntity.setName("table");
        List<ManagerTableEntity> tableEntities = Lists.newArrayList(tableEntity);
        when(tableRepository.getByDbId(dbId)).thenReturn(tableEntities);
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            List<Map<String, Object>> result = metadataService.getTableListByDb(dbId, userId);
            Assert.assertEquals(result.size(), 1);
        } catch (Exception e) {
            log.error("Get database table list error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void getTableInfoTest() {
        log.debug("Get table info test");
        int clusterId = 1;
        int userId = 2;
        int tableId = 3;
        int dbId = 4;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // get Doris information_schema table
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            TableResp result = metadataService.getTableInfo(-1, userId);
            Assert.assertEquals(result.getName(), "character_sets");
            Assert.assertEquals(result.getDbId(), -1);
        } catch (Exception e) {
            log.error("Get table info error {}.", e.getMessage());
            e.printStackTrace();
        }

        // get other table
        // mock table,description empty
        String tableName = "table";
        ManagerTableEntity table = new ManagerTableEntity();
        table.setName(tableName);
        table.setId(tableId);
        table.setDbId(dbId);
        when(tableRepository.findById(tableId)).thenReturn(Optional.of(table));
        // mock database
        ManagerDatabaseEntity database = new ManagerDatabaseEntity();
        database.setClusterId(clusterId);
        database.setId(dbId);
        database.setName("db");
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(database);
            TableResp result = metadataService.getTableInfo(tableId, userId);
            Assert.assertEquals(result.getName(), tableName);
            Assert.assertEquals(result.getDbId(), dbId);
        } catch (Exception e) {
            log.error("Get table info error {}.", e.getMessage());
            e.printStackTrace();
        }

        // mock table,description not empty
        DataDescription description = new DataDescription();
        description.setUserName("userName");
        description.setDescription("test desc");
        table.setDescription(JSON.toJSONString(description));
        when(tableRepository.findById(tableId)).thenReturn(Optional.of(table));

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(databuildComponent.checkClusterDatabase(dbId, clusterId)).thenReturn(database);
            TableResp result = metadataService.getTableInfo(tableId, userId);
            Assert.assertEquals(result.getCreator(), description.getUserName());
            Assert.assertEquals(result.getDescribe(), description.getDescription());
        } catch (Exception e) {
            log.error("Get table info error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void getTableSchemaAndFieldListTest() {
        log.debug("Get table schema and field list test");
        int clusterId = 1;
        int userId = 2;
        int tableId = 3;
        int dbId = 4;

        // mock cluster
        ClusterInfoEntity clusterInfo = mockClusterInfo(clusterId);

        // mock schema
        TableSchemaInfo.TableSchema tableSchema = new TableSchemaInfo.TableSchema();
        TableSchemaInfo.Schema schema1 = new TableSchemaInfo.Schema();
        schema1.setIsNull("null");
        schema1.setField("field1");
        schema1.setType("int");
        schema1.setKey("true");
        List<TableSchemaInfo.Schema> schema = Lists.newArrayList(schema1);
        tableSchema.setSchema(schema);

        // get Doris information_schema table schema
        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            when(metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, ConstantDef.MYSQL_DEFAULT_SCHEMA,
                    "character_sets", clusterInfo)).thenReturn(tableSchema);
            TableSchemaInfo.TableSchema result = metadataService.getTableSchema(-1, userId);
            Assert.assertEquals(result.getSchema().size(), tableSchema.getSchema().size());

            List<String> fieldList = metadataService.getFieldListByTable(-1, userId);
            Assert.assertEquals(fieldList, tableSchema.fieldList());
        } catch (Exception e) {
            log.error("Get table schema or field list error {}.", e.getMessage());
            e.printStackTrace();
        }

        // get Doris other table schema
        // mock table
        String tableName = "table";
        ManagerTableEntity table = new ManagerTableEntity();
        table.setName(tableName);
        table.setId(tableId);
        table.setDbId(dbId);
        table.setBaseIndex(true);
        table.setKeyType("agg");
        when(tableRepository.findById(tableId)).thenReturn(Optional.of(table));
        // mock fields
        ManagerFieldEntity fieldEntity = new ManagerFieldEntity();
        fieldEntity.setName("field1");
        fieldEntity.setId(1);
        fieldEntity.setTableId(tableId);
        fieldEntity.setIsNull("null");
        fieldEntity.setKey("true");
        List<ManagerFieldEntity> fieldEntities = Lists.newArrayList(fieldEntity);
        when(fieldRepository.getByTableId(tableId)).thenReturn(fieldEntities);

        try {
            when(clusterUserComponent.getClusterByUserId(userId)).thenReturn(clusterInfo);
            TableSchemaInfo.TableSchema result = metadataService.getTableSchema(tableId, userId);
            Assert.assertEquals(result.getSchema().size(), fieldEntities.size());

            List<String> fieldList = metadataService.getFieldListByTable(tableId, userId);
            Assert.assertEquals(fieldList.size(), fieldEntities.size());
        } catch (Exception e) {
            log.error("Get table schema or field list error {}.", e.getMessage());
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

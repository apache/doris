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

package org.apache.doris.stack.component;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.palo.TableSchemaInfo;
import org.apache.doris.stack.connector.PaloMetaInfoClient;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(JUnit4.class)
@Slf4j
public class ManagerMetaSyncComponentTest {

    @InjectMocks
    private ManagerMetaSyncComponent syncComponent;

    @Mock
    private ManagerDatabaseRepository databaseRepository;

    @Mock
    private ManagerTableRepository tableRepository;

    @Mock
    private ManagerFieldRepository fieldRepository;

    @Mock
    private PaloMetaInfoClient metaInfoClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    // Test deleting metadata for cluster
    @Test
    public void deleteClusterMetadataTest() {
        ClusterInfoEntity clusterInfo = mockClusterInfo();

        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setClusterId(clusterInfo.getId());
        databaseEntity.setId(1);
        databaseEntity.setName("db");
        List<ManagerDatabaseEntity> databaseEntities = Lists.newArrayList(databaseEntity);

        when(databaseRepository.getByClusterId(clusterInfo.getId())).thenReturn(databaseEntities);

        try {
            syncComponent.deleteClusterMetadata(clusterInfo);
        } catch (Exception e) {
            log.error("delete meta error.");
            e.printStackTrace();
        }
    }

    /**
     * Test add metadata
     */
    @Test
    public void syncPaloClusterMetadataTestAddDb() {
        ClusterInfoEntity clusterInfo = mockClusterInfo();
        int clusterId = clusterInfo.getId();

        try {
            // Engine latest database list
            String dbName = "a";
            List<String> newDbs = Lists.newArrayList(dbName, ConstantDef.MYSQL_DEFAULT_SCHEMA);
            when(metaInfoClient.getDatabaseList(ConstantDef.DORIS_DEFAULT_NS, clusterInfo)).thenReturn(newDbs);

            // The original storage DB list is empty
            when(databaseRepository.getNameByClusterId(clusterId)).thenReturn(new ArrayList<>());

            // The existing DB is empty
            when(databaseRepository.getByClusterIdAndName(clusterId, dbName)).thenReturn(new ArrayList<>());

            // Storage metadata exception
            syncComponent.syncPaloClusterMetadata(clusterInfo);

            // Construct new DB
            ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity(clusterId, dbName, "");
            databaseEntity.setId(1);
            when(databaseRepository.save(any())).thenReturn(databaseEntity);

            // Add constructed table information
            String tableName = "t1";
            List<String> tableList = Lists.newArrayList(tableName);
            when(metaInfoClient.getTableList(ConstantDef.DORIS_DEFAULT_NS, dbName, clusterInfo)).thenReturn(tableList);
            when(tableRepository.getByDbIdAndName(databaseEntity.getId(), tableName)).thenReturn(new ArrayList<>());

            mockAddTable(dbName, tableName, clusterInfo, databaseEntity.getId());

            syncComponent.syncPaloClusterMetadata(clusterInfo);

        } catch (Exception e) {
            log.error("add db error");
        }
    }

    @Test
    public void syncPaloClusterMetadataTestReduceDb() {
        ClusterInfoEntity clusterInfo = mockClusterInfo();
        int clusterId = clusterInfo.getId();

        try {
            // Engine latest database list
            List<String> newDbs = Lists.newArrayList(ConstantDef.MYSQL_DEFAULT_SCHEMA);
            when(metaInfoClient.getDatabaseList(ConstantDef.DORIS_DEFAULT_NS, clusterInfo)).thenReturn(newDbs);

            // The original storage DB list is empty
            String dbName = "a";
            List<String> oldDbs = Lists.newArrayList(dbName);
            when(databaseRepository.getNameByClusterId(clusterId)).thenReturn(oldDbs);

            // The existing DB is a
            ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
            databaseEntity.setId(1);
            databaseEntity.setName(dbName);
            databaseEntity.setClusterId(clusterId);
            List<ManagerDatabaseEntity> databaseEntities = Lists.newArrayList(databaseEntity);
            when(databaseRepository.getByClusterIdAndName(clusterId, dbName)).thenReturn(databaseEntities);

            // Construct DB existing table
            String tableName = "t1";
            ManagerTableEntity tableEntity = new ManagerTableEntity();
            tableEntity.setId(1);
            tableEntity.setDbId(databaseEntity.getId());
            tableEntity.setName(tableName);
            List<ManagerTableEntity> tableEntities = Lists.newArrayList(tableEntity);
            when(tableRepository.getByDbId(databaseEntity.getId())).thenReturn(tableEntities);

            syncComponent.syncPaloClusterMetadata(clusterInfo);

        } catch (Exception e) {
            log.error("add db error");
        }
    }

    @Test
    public void syncPaloClusterMetadataTestUpdateDb() {
        ClusterInfoEntity clusterInfo = mockClusterInfo();
        int clusterId = clusterInfo.getId();

        try {
            // Engine latest database list
            String dbName = "a";
            List<String> newDbs = Lists.newArrayList(dbName, ConstantDef.MYSQL_DEFAULT_SCHEMA);
            when(metaInfoClient.getDatabaseList(ConstantDef.DORIS_DEFAULT_NS, clusterInfo)).thenReturn(newDbs);

            // The original storage DB list is a
            List<String> oldDbs = Lists.newArrayList(dbName);
            when(databaseRepository.getNameByClusterId(clusterId)).thenReturn(oldDbs);

            // The existing DB is a
            int dbId = 1;
            ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
            databaseEntity.setId(dbId);
            databaseEntity.setName(dbName);
            databaseEntity.setClusterId(clusterId);
            List<ManagerDatabaseEntity> databaseEntities = Lists.newArrayList(databaseEntity);
            when(databaseRepository.getByClusterIdAndName(clusterId, dbName)).thenReturn(databaseEntities);

            // Construct DB latest table list
            String tableAddName = "t1";
            String tableExistName = "t2";
            List<String> tableList = Lists.newArrayList(tableAddName, tableExistName);
            when(metaInfoClient.getTableList(ConstantDef.DORIS_DEFAULT_NS, dbName, clusterInfo)).thenReturn(tableList);

            // Construct the table list that DB already exists
            ManagerTableEntity tableExist = new ManagerTableEntity();
            tableExist.setId(1);
            tableExist.setDbId(dbId);
            tableExist.setName(tableExistName);
            String tableReduceName = "t3";
            ManagerTableEntity tableReduce = new ManagerTableEntity();
            tableReduce.setId(2);
            tableReduce.setDbId(dbId);
            tableReduce.setName(tableReduceName);
            List<ManagerTableEntity> tableEntities = Lists.newArrayList(tableExist, tableReduce);
            when(tableRepository.getByDbId(dbId)).thenReturn(tableEntities);

            // Construct the information to add a table
            mockAddTable(dbName, tableAddName, clusterInfo, dbId);

            // Construct field information of existing table
            // The field has not changed
            TableSchemaInfo.TableSchema tableSchema = new TableSchemaInfo.TableSchema();
            TableSchemaInfo.Schema schema = new TableSchemaInfo.Schema();
            schema.setType("INT");
            schema.setField("f1");
            schema.setIsNull("false");
            List<TableSchemaInfo.Schema> schemas = Lists.newArrayList(schema);
            tableSchema.setSchema(schemas);
            when(metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, dbName, tableExistName, clusterInfo)).thenReturn(tableSchema);

            ManagerFieldEntity fieldEntity = new ManagerFieldEntity();
            fieldEntity.setId(1);
            fieldEntity.setName("f1");
            List<ManagerFieldEntity> fieldEntities = Lists.newArrayList(fieldEntity);
            when(fieldRepository.getByTableId(tableExist.getId())).thenReturn(fieldEntities);

            syncComponent.syncPaloClusterMetadata(clusterInfo);

            // field add
            when(fieldRepository.getByTableId(tableExist.getId())).thenReturn(new ArrayList<>());
            syncComponent.syncPaloClusterMetadata(clusterInfo);

        } catch (Exception e) {
            log.error("add db error");
        }
    }

    @Test
    public void addDatabaseTest() {
        int nsId = 0;
        int clusterId = 1;
        String db = "db1";
        String description = "desc";
        int dbId = 2;
        int exsitDbId = 1;

        // storage exception
        try {
            syncComponent.addDatabase(db, description, nsId, clusterId);
        } catch (Exception e) {
            log.debug("Add database exception {}.", e.getMessage());
        }

        // db already exists
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setId(exsitDbId);
        databaseEntity.setName(db);
        databaseEntity.setClusterId(clusterId);
        List<ManagerDatabaseEntity> databaseEntities = Lists.newArrayList(databaseEntity);
        when(databaseRepository.getByClusterIdAndName(clusterId, db)).thenReturn(databaseEntities);
        try {
            int result = syncComponent.addDatabase(db, description, nsId, clusterId);
            Assert.assertEquals(exsitDbId, result);
        } catch (Exception e) {
            log.error("Add database exception {}.", e.getMessage());
            e.printStackTrace();
        }

        // add db
        ManagerDatabaseEntity databaseNew = new ManagerDatabaseEntity(clusterId, db, description);
        databaseNew.setId(dbId);
        when(databaseRepository.save(any())).thenReturn(databaseNew);

        when(databaseRepository.getByClusterIdAndName(clusterId, db)).thenReturn(new ArrayList<>());

        try {
            int result = syncComponent.addDatabase(db, description, nsId, clusterId);
            Assert.assertEquals(dbId, result);
        } catch (Exception e) {
            log.error("Add database exception {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void addTableTest() {
        int dbId = 1;
        String tableName = "t1";
        String description = "desc";

        TableSchemaInfo.TableSchema tableSchema = new TableSchemaInfo.TableSchema();
        TableSchemaInfo.Schema schema = new TableSchemaInfo.Schema();
        schema.setType("INT");
        schema.setField("f1");
        schema.setIsNull("false");
        List<TableSchemaInfo.Schema> schemas = Lists.newArrayList(schema);
        tableSchema.setSchema(schemas);

        // table already exists
        int tableId = 1;
        ManagerTableEntity tableEntity = new ManagerTableEntity();
        tableEntity.setName(tableName);
        tableEntity.setId(tableId);
        tableEntity.setDbId(dbId);
        List<ManagerTableEntity> tableEntities = Lists.newArrayList(tableEntity);
        when(tableRepository.getByDbIdAndName(dbId, tableName)).thenReturn(tableEntities);

        try {
            int result = syncComponent.addTable(dbId, tableName, description, tableSchema);
            Assert.assertEquals(tableId, result);
        } catch (Exception e) {
            log.error("Add table exception {}.", e.getMessage());
            e.printStackTrace();
        }

        // table storage excpetion
        when(tableRepository.getByDbIdAndName(dbId, tableName)).thenReturn(new ArrayList<>());
        try {
            int result = syncComponent.addTable(dbId, tableName, description, tableSchema);
        } catch (Exception e) {
            log.debug("Add table exception {}.", e.getMessage());
        }

        // table storage
        int tableNewId = 2;
        tableEntity.setId(tableNewId);
        when(tableRepository.save(any())).thenReturn(tableEntity);
        try {
            int result = syncComponent.addTable(dbId, tableName, description, tableSchema);
            Assert.assertEquals(tableNewId, result);
        } catch (Exception e) {
            log.error("Add table exception {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void deleteDatabaseTest() {
        int dbId = 1;
        int clusterId = 1;
        String dbName = "db";

        // construction database
        ManagerDatabaseEntity databaseEntity = new ManagerDatabaseEntity();
        databaseEntity.setId(dbId);
        databaseEntity.setName(dbName);
        databaseEntity.setClusterId(clusterId);
        when(databaseRepository.findById(dbId)).thenReturn(Optional.of(databaseEntity));

        // delete exception
        List<ManagerDatabaseEntity> databaseEntities = new ArrayList<>();
        databaseEntities.add(null);
        when(databaseRepository.getByClusterIdAndName(clusterId, dbName)).thenReturn(databaseEntities);

        try {
            syncComponent.deleteDatabase(clusterId, dbId);
        } catch (Exception e) {
            log.debug("delete error {}.", e.getMessage());
        }

        // delete
        databaseEntities = Lists.newArrayList(databaseEntity);
        when(databaseRepository.getByClusterIdAndName(clusterId, dbName)).thenReturn(databaseEntities);
        try {
            syncComponent.deleteDatabase(clusterId, dbId);
        } catch (Exception e) {
            log.error("delete error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    // construction doris cluster
    private ClusterInfoEntity mockClusterInfo() {
        ClusterInfoEntity clusterInfo = new ClusterInfoEntity();
        clusterInfo.setId(1);
        clusterInfo.setName("doris1");
        clusterInfo.setAddress("10.23.32.32");
        clusterInfo.setHttpPort(8030);
        clusterInfo.setQueryPort(8031);
        clusterInfo.setUser("admin");
        clusterInfo.setPasswd("1234");
        clusterInfo.setTimezone("Asia/Shanghai");
        return clusterInfo;
    }

    private void mockAddTable(String dbName, String tableAddName,
                              ClusterInfoEntity clusterInfo, int dbId) throws Exception {
        TableSchemaInfo.TableSchema tableSchema = new TableSchemaInfo.TableSchema();
        TableSchemaInfo.Schema schema = new TableSchemaInfo.Schema();
        schema.setType("INT");
        schema.setField("f1");
        schema.setIsNull("false");
        List<TableSchemaInfo.Schema> schemas = Lists.newArrayList(schema);
        tableSchema.setSchema(schemas);
        when(metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, dbName, tableAddName, clusterInfo)).thenReturn(tableSchema);

        ManagerTableEntity tableEntity = new ManagerTableEntity(dbId, tableAddName, "", tableSchema);
        tableEntity.setId(1);
        when(tableRepository.save(any())).thenReturn(tableEntity);

        // construction table fields
        when(fieldRepository.getByTableId(tableEntity.getId())).thenReturn(new ArrayList<>());
    }

}

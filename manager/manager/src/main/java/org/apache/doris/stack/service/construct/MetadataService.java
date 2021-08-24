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

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.palo.TableSchemaInfo;
import org.apache.doris.stack.model.response.construct.DatabaseResp;
import org.apache.doris.stack.model.response.construct.TableResp;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.component.ManagerMetaSyncComponent;
import org.apache.doris.stack.component.ManagerMetaSynchronizer;
import org.apache.doris.stack.connector.PaloMetaInfoClient;
import org.apache.doris.stack.dao.ClusterInfoRepository;
import org.apache.doris.stack.dao.ManagerDatabaseRepository;
import org.apache.doris.stack.dao.ManagerFieldRepository;
import org.apache.doris.stack.dao.ManagerTableRepository;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.entity.ManagerFieldEntity;
import org.apache.doris.stack.entity.ManagerTableEntity;
import org.apache.doris.stack.service.BaseService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class MetadataService extends BaseService {

    @Autowired
    private ClusterInfoRepository clusterInfoRepository;

    @Autowired
    private ManagerMetaSyncComponent syncComponent;

    @Autowired
    private ManagerDatabaseRepository databaseRepository;

    @Autowired
    private ManagerTableRepository tableRepository;

    @Autowired
    private ManagerFieldRepository fieldRepository;

    @Autowired
    private PaloMetaInfoClient metaInfoClient;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    @Autowired
    private DatabuildComponent databuildComponent;

    /**
     * Asynchronous execution method
     *
     * Synchronize all metadata of the Doris cluster backend in the current space of the manager every two hours
     */
    @Scheduled(cron = "0 */240 * * * ?")
    @Transactional
    public void syncMetadata() {
        log.info("Background synchronization metadata");
        try {
            ExecutorService threadPool = Executors.newFixedThreadPool(1);
            List<ClusterInfoEntity> clusters = clusterInfoRepository.findAll();
            ManagerMetaSynchronizer synchronizer = new ManagerMetaSynchronizer(clusters, syncComponent);
            threadPool.submit(synchronizer);
        } catch (Exception e) {
            log.error("Sync metadata exception");
            e.printStackTrace();
        }
    }

    @Transactional
    public void syncMetadataByUserId(int studioUserId) throws Exception {
        log.info("synchronization metadata by userId {}.", studioUserId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        syncMetadataByCluster(clusterInfo);
    }

    public void syncMetadataByCluster(ClusterInfoEntity clusterInfo) {
        log.debug("Sync metabadata for palo space cluster {}.", clusterInfo.getId());
        List<ClusterInfoEntity> clusters = Lists.newArrayList(clusterInfo);
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        ManagerMetaSynchronizer synchronizer = new ManagerMetaSynchronizer(clusters, syncComponent);
        threadPool.submit(synchronizer);
    }

    /**
     * Get the list of databases according to NS
     * TODO:Currently, Doris does not support multiple ns, which is now the default value
     *
     * @param ns
     * @param studioUserId
     * @return
     */
    @Transactional
    public List<Map<String, Object>> getDatabaseListByNs(int ns, int studioUserId) throws Exception {
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);

        int clusterId = clusterInfo.getId();
        log.debug("User {} get cluster {} all databases.", studioUserId, clusterId);

        List<ManagerDatabaseEntity> databaseEntities = databaseRepository.getByClusterId(clusterId);
        List<Map<String, Object>> result = Lists.newArrayList();
        for (ManagerDatabaseEntity databaseEntity : databaseEntities) {
            Map<String, Object> dbInfo = Maps.newHashMap();
            dbInfo.put("id", databaseEntity.getId());
            dbInfo.put("name", databaseEntity.getName());
            result.add(dbInfo);
        }
        // add information_schema
        Map<String, Object> metaDb = Maps.newHashMap();
        metaDb.put("id", ConstantDef.MYSQL_SCHEMA_DB_ID);
        metaDb.put("name", ConstantDef.MYSQL_DEFAULT_SCHEMA);
        result.add(metaDb);
        return result;
    }

    /**
     * Get database details
     *
     * @param dbId
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public DatabaseResp getDatabaseInfo(int dbId, int studioUserId) throws Exception {
        log.debug("User {} get database {} info.", studioUserId, dbId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);

        if (dbId < 0) {
            return new DatabaseResp(ConstantDef.MYSQL_DEFAULT_SCHEMA, "palo metadata database",
                    clusterInfo.getUser(), null);
        } else {
            ManagerDatabaseEntity database = databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());

            DataDescription description = null;
            if (database.getDescription() == null || database.getDescription().isEmpty()) {
                description = new DataDescription();
            } else {
                description = JSON.parseObject(database.getDescription(), DataDescription.class);
            }
            DatabaseResp resp = new DatabaseResp(database.getName(), description.getDescription(),
                    description.getUserName(), database.getCreatedAt());
            return resp;
        }
    }

    /**
     * Get the table list of database
     *
     * @param dbId
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public List<Map<String, Object>> getTableListByDb(int dbId, int studioUserId) throws Exception {
        log.debug("User {} get table list for database {}.", studioUserId, dbId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);

        List<Map<String, Object>> result = Lists.newArrayList();
        if (dbId < 0) {
            for (int key : ConstantDef.MYSQL_METADATA_TABLE.keySet()) {
                Map<String, Object> tableInfo = Maps.newHashMap();
                tableInfo.put("id", key);
                tableInfo.put("name", ConstantDef.MYSQL_METADATA_TABLE.get(key));
                result.add(tableInfo);
            }
        } else {
            databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());

            List<ManagerTableEntity> tableEntities = tableRepository.getByDbId(dbId);
            for (ManagerTableEntity table : tableEntities) {
                Map<String, Object> tableInfo = Maps.newHashMap();
                tableInfo.put("id", table.getId());
                tableInfo.put("name", table.getName());
                result.add(tableInfo);
            }
        }

        return result;
    }

    /**
     * Get table details
     *
     * @param tableId
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public TableResp getTableInfo(int tableId, int studioUserId) throws Exception {
        log.debug("User {} get table {} info.", studioUserId, tableId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        if (tableId < 0) {
            return new TableResp(ConstantDef.MYSQL_METADATA_TABLE.get(tableId), "palo metadata table", "root",
                    null, null, ConstantDef.MYSQL_SCHEMA_DB_ID, ConstantDef.MYSQL_DEFAULT_SCHEMA);
        } else {
            ManagerTableEntity table = tableRepository.findById(tableId).get();

            ManagerDatabaseEntity database =
                    databuildComponent.checkClusterDatabase(table.getDbId(), clusterInfo.getId());

            DataDescription description = null;
            if (table.getDescription() == null || table.getDescription().isEmpty()) {
                description = new DataDescription();
            } else {
                description = JSON.parseObject(table.getDescription(), DataDescription.class);
            }

            TableResp resp = new TableResp(table.getName(), description.getDescription(), description.getUserName(),
                    table.getCreatedAt(), table.getUpdatedAt(), database.getId(), database.getName());
            return resp;
        }
    }

    /**
     * Gets the list of fields in the table
     *
     * @param tableId
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public List<String> getFieldListByTable(int tableId, int studioUserId) throws Exception {
        log.debug("User {} get table {} field list.", studioUserId, tableId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        if (tableId < 0) {
            String dbName = ConstantDef.MYSQL_DEFAULT_SCHEMA;
            String tableName = ConstantDef.MYSQL_METADATA_TABLE.get(tableId);
            TableSchemaInfo.TableSchema schema = metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS,
                    dbName, tableName, clusterInfo);
            return schema.fieldList();
        } else {
            // permisssion check
            ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
            databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());

            List<ManagerFieldEntity> fieldEntities = fieldRepository.getByTableId(tableId);

            List<String> fieldNameList = new ArrayList<>();
            for (ManagerFieldEntity fieldEntity : fieldEntities) {
                fieldNameList.add(fieldEntity.getName());
            }
            return fieldNameList;
        }
    }

    /**
     * Get schema information of the table
     *
     * @param tableId
     * @param studioUserId
     * @return
     * @throws Exception
     */
    @Transactional
    public TableSchemaInfo.TableSchema getTableSchema(int tableId, int studioUserId) throws Exception {
        log.debug("User {} get table {} Schema info.", studioUserId, tableId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        if (tableId < 0) {
            String dbName = ConstantDef.MYSQL_DEFAULT_SCHEMA;
            String tableName = ConstantDef.MYSQL_METADATA_TABLE.get(tableId);
            return metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS,
                    dbName, tableName, clusterInfo);
        } else {
            // permission check
            ManagerTableEntity tableEntity = tableRepository.findById(tableId).get();
            databuildComponent.checkClusterDatabase(tableEntity.getDbId(), clusterInfo.getId());

            // Construct response data
            TableSchemaInfo.TableSchema tableSchema = new TableSchemaInfo.TableSchema();
            tableSchema.setBaseIndex(tableEntity.isBaseIndex());
            tableSchema.setKeyType(tableEntity.getKeyType());

            List<TableSchemaInfo.Schema> schema = new ArrayList<>();
            List<ManagerFieldEntity> fieldEntities = fieldRepository.getByTableId(tableId);
            for (ManagerFieldEntity fieldEntity : fieldEntities) {
                schema.add(fieldEntity.transToModel());
            }
            tableSchema.setSchema(schema);

            return tableSchema;
        }
    }
}

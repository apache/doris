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

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.palo.TableSchemaInfo;
import org.apache.doris.stack.util.ListUtil;
import org.apache.doris.stack.connector.PaloMetaInfoClient;
import org.apache.doris.stack.dao.ManagerDatabaseRepository;
import org.apache.doris.stack.dao.ManagerFieldRepository;
import org.apache.doris.stack.dao.ManagerTableRepository;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.entity.ManagerFieldEntity;
import org.apache.doris.stack.entity.ManagerTableEntity;
import org.apache.doris.stack.exception.MetaDataSyncException;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class ManagerMetaSyncComponent extends BaseService {

    @Autowired
    private ManagerDatabaseRepository databaseRepository;

    @Autowired
    private ManagerTableRepository tableRepository;

    @Autowired
    private ManagerFieldRepository fieldRepository;

    @Autowired
    private PaloMetaInfoClient metaInfoClient;

    /**
     * Delete all manaager metadata in the space
     * @param clusterInfo
     */
    public void deleteClusterMetadata(ClusterInfoEntity clusterInfo) throws Exception {
        int clusterId = clusterInfo.getId();
        log.debug("Delete cluster {} manager metadata.", clusterId);
        // Delete all databases metadata in the space
        List<ManagerDatabaseEntity> databaseEntities = databaseRepository.getByClusterId(clusterId);
        for (ManagerDatabaseEntity databaseEntity : databaseEntities) {
            deleteDatabase(databaseEntity.getName(), clusterId);
        }
    }

    /**
     * Synchronously update the metadata information of the doris cluster
     *
     * @param clusterInfo
     * @throws Exception
     */
    public void syncPaloClusterMetadata(ClusterInfoEntity clusterInfo) throws Exception {
        int clusterId = clusterInfo.getId();
        try {
            log.info("Start to sync palo cluster {} metadata to manager.", clusterId);
            List<String> databaseList = metaInfoClient.getDatabaseList(ConstantDef.DORIS_DEFAULT_NS, clusterInfo);

            // Get the list of all databases in Doris cluster
            List<String> oldDatabaseList = databaseRepository.getNameByClusterId(clusterId);

            // Get the newly added database, and the corresponding table and field are newly added
            List<String> addDbList = ListUtil.getAddList(databaseList, oldDatabaseList);
            log.info("Get new database {}.", addDbList);
            for (String db : addDbList) {
                if (db.equals(ConstantDef.MYSQL_DEFAULT_SCHEMA)) {
                    continue;
                }
                int dbId = addDatabase(db, "", 0, clusterId);
                List<String> tableList = metaInfoClient.getTableList(ConstantDef.DORIS_DEFAULT_NS, db, clusterInfo);
                for (String table : tableList) {
                    TableSchemaInfo.TableSchema tableSchema =
                            metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, db, table, clusterInfo);
                    int tableId = addTable(dbId, table, "", tableSchema);
                    addTableFieldList(tableId, tableSchema);
                }
            }

            // Get the reduced database, and delete the corresponding table and field
            List<String> reduceDbList = ListUtil.getReduceList(databaseList, oldDatabaseList);
            log.info("Get reduce database {}.", reduceDbList);
            for (String db : reduceDbList) {
                deleteDatabase(db, clusterId);
            }

            // Get the original database, compare whether the table and field in it are updated,
            // and modify them if they are updated
            List<String> existDbList = ListUtil.getExistList(databaseList, oldDatabaseList);
            log.info("Get exist database {}.", existDbList);
            for (String db : existDbList) {
                ManagerDatabaseEntity databaseEntity = databaseRepository.getByClusterIdAndName(clusterId, db).get(0);
                int dbId = databaseEntity.getId();
                log.debug("update database {}", dbId);
                List<String> tableList = metaInfoClient.getTableList(ConstantDef.DORIS_DEFAULT_NS, db, clusterInfo);

                List<ManagerTableEntity> tableEntities = tableRepository.getByDbId(dbId);
                List<String> oldTableList = new ArrayList<>();
                for (ManagerTableEntity tableEntity : tableEntities) {
                    oldTableList.add(tableEntity.getName());
                }

                // Get the newly added table and get the corresponding newly added field information
                List<String> addTableList = ListUtil.getAddList(tableList, oldTableList);
                log.info("Get new table {}.", addTableList);
                for (String table : addTableList) {
                    TableSchemaInfo.TableSchema tableSchema =
                            metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, db, table, clusterInfo);
                    int tableId = addTable(dbId, table, "", tableSchema);
                    addTableFieldList(tableId, tableSchema);
                }

                // Get the deleted table and delete the corresponding field information
                List<String> reduceTableList = ListUtil.getReduceList(tableList, oldTableList);
                log.info("Get reduce table {}.", reduceTableList);
                for (String table : reduceTableList) {
                    int tableId = getTableId(tableEntities, table);
                    deleteTable(tableId);
                }

                // Get the original table and compare whether the field is updated. If it is updated, modify it.
                // Here, the way to modify the field information is to delete it all first and then store it again
                List<String> existTableList = ListUtil.getExistList(tableList, oldTableList);
                log.info("Get exist table {}.", existTableList);
                for (String table : existTableList) {
                    TableSchemaInfo.TableSchema tableSchema =
                            metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, db, table, clusterInfo);

                    List<String> tableFields = tableSchema.fieldList();

                    int tableId = getTableId(tableEntities, table);
                    List<ManagerFieldEntity> fieldEntities = fieldRepository.getByTableId(tableId);
                    List<String> oldTableFields = new ArrayList<>();
                    for (ManagerFieldEntity fieldEntity : fieldEntities) {
                        oldTableFields.add(fieldEntity.getName());
                    }

                    List<String> existTableFields = ListUtil.getExistList(tableFields, oldTableFields);
                    // If the field field information of the table is not modified
                    // TODO:At present, only the number and name of fields are compared
                    if (existTableFields.size() == tableFields.size() && tableFields.size() == oldTableFields.size()) {
                        log.debug("The table {} field information has not been modified.", tableId);
                    } else {
                        log.debug("The table {} field information has been modified, update fields", tableId);
                        fieldRepository.deleteByTableId(tableId);
                        addTableFieldList(tableId, tableSchema);
                    }
                }
            }
            log.info("End to sync palo cluster {} metadata to studio.", clusterId);
        } catch (Exception e) {
            log.error("Sync palo cluster {} metadata exception {}", clusterId, e);
            throw new MetaDataSyncException("Sync palo cluster metadata error.");
        }
    }

    /**
     * Add Doris namespace information
     * TODO:Subsequent increase
     *
     * @param ns
     * @param clusterId
     * @return
     */
    public int addNameSpace(String ns, long clusterId) {
        return 0;
    }

    /**
     * Judge whether the current DB already exists according to the db name and cluster information.
     * If so, do not operate. If not,you need to add a metadata storage backend that stores database
     * metadata information to the manager
     * TODO:Currently, a Doris cluster has only one default ns by default, so the database name will not be repeated.
     * TODO: You can judge whether the database exists according to the name;
     *
     * @param db
     * @param description
     * @param nsId
     * @param clusterId
     * @return
     * @throws Exception
     */
    public int addDatabase(String db, String description, int nsId, int clusterId) throws Exception {
        try {
            // Judge whether the database has been cached
            List<ManagerDatabaseEntity> existDb = databaseRepository.getByClusterIdAndName(clusterId, db);
            if (existDb != null && !existDb.isEmpty()) {
                log.warn("the db {} is exist", db);
                return existDb.get(0).getId();
            }

            // Initialize the metadata database of the manager
            ManagerDatabaseEntity databaseEntity =
                    new ManagerDatabaseEntity(clusterId, db, description);

            // Metadata database for storage manager
            int dbId = databaseRepository.save(databaseEntity).getId();

            log.debug("add new database {}.", dbId);
            return dbId;
        } catch (Exception e) {
            log.error("store database metadata exception {}.", e);
            throw new MetaDataSyncException("store database metadata error:" + e.getMessage());
        }
    }

    public void deleteDatabase(int clusterId, int dbId) throws Exception {
        ManagerDatabaseEntity databaseEntity = databaseRepository.findById(dbId).get();
        deleteDatabase(databaseEntity.getName(), clusterId);
    }

    /**
     * If the database is deleted,
     * delete it from the metadata information and delete the corresponding table and field information
     *
     * @throws Exception
     */
    private void deleteDatabase(String db, int clusterId) throws Exception {
        log.debug("delete cluster {} database {} metadata", clusterId, db);
        try {
            // Get database information
            List<ManagerDatabaseEntity> databaseEntities = databaseRepository.getByClusterIdAndName(clusterId, db);
            if (databaseEntities == null || databaseEntities.isEmpty()) {
                log.warn("the db {} is not exist", db);
                return;
            }
            ManagerDatabaseEntity databaseEntity = databaseEntities.get(0);
            int dbId = databaseEntity.getId();

            log.debug("delete palo cluster {} database {} tables.", clusterId, dbId);
            List<ManagerTableEntity> tableEntities = tableRepository.getByDbId(dbId);
            for (ManagerTableEntity tableEntity : tableEntities) {
                deleteTable(tableEntity.getId());
            }
            databaseRepository.deleteById(dbId);
            log.debug("delete palo cluster {} database {}.", clusterId, dbId);
        } catch (Exception e) {
            log.error("delete database metadata exception {}.", e);
            throw new MetaDataSyncException(e.getMessage());
        }
    }

    /**
     * Add table information
     *
     * @param dbId
     * @param name
     * @param description
     * @return
     * @throws Exception
     */
    public int addTable(int dbId, String name, String description,
                        TableSchemaInfo.TableSchema tableSchema) throws Exception {
        try {
            // Determine whether the table exists
            List<ManagerTableEntity> existTable = tableRepository.getByDbIdAndName(dbId, name);
            if (existTable != null && !existTable.isEmpty()) {
                log.warn("the db {} table {} is exist", dbId, name);
                return existTable.get(0).getId();
            }

            // Initialize and save table information
            ManagerTableEntity tableEntity = new ManagerTableEntity(dbId, name, description, tableSchema);
            int tableId = tableRepository.save(tableEntity).getId();
            log.debug("add new table {}.", tableId);
            return tableId;
        } catch (Exception e) {
            log.error("store table metadata exception {}.", e);
            throw new MetaDataSyncException(e.getMessage());
        }
    }

    /**
     * Delete the table information and its corresponding field list
     *
     * @param tableId
     * @throws Exception
     */
    private void deleteTable(int tableId) throws Exception {
        try {
            log.debug("delete table {} and fields.", tableId);
            fieldRepository.deleteByTableId(tableId);
            tableRepository.deleteById(tableId);
        } catch (Exception e) {
            log.error("delete table metadata exception {}.", e);
            throw new MetaDataSyncException(e.getMessage());
        }
    }

    /**
     * Add field list information of table
     *
     * @param tableId
     * @param tableSchema
     * @throws Exception
     */
    public void addTableFieldList(int tableId, TableSchemaInfo.TableSchema tableSchema) throws Exception {
        try {
            log.debug("add table {} fieldList.", tableId);
            List<TableSchemaInfo.Schema> fields = tableSchema.getSchema();
            List<ManagerFieldEntity> fieldEntities = fieldRepository.getByTableId(tableId);
            if (fieldEntities.size() != 0) {
                log.warn("The table fields are exist.");
                return;
            }
            int position = 0;
            for (TableSchemaInfo.Schema field : fields) {
                ManagerFieldEntity fieldEntity = new ManagerFieldEntity(tableId, field, position);
                fieldRepository.save(fieldEntity);
                position++;
                log.debug("Add table {} field {} success.", tableId, field.getField());
            }
            log.debug("Add table {} fields success.", tableId);
        } catch (Exception e) {
            log.error("store field metadata exception {}.", e);
            throw new MetaDataSyncException(e.getMessage());
        }
    }

    // By default, the table names in a database are not duplicate
    private int getTableId(List<ManagerTableEntity> tableEntities, String name) {
        for (ManagerTableEntity tableEntity : tableEntities) {
            if (tableEntity.getName().equals(name)) {
                return tableEntity.getId();
            }
        }
        return -1;
    }
}

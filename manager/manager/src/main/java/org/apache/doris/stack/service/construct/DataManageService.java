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
import org.apache.doris.stack.exception.MetaDataSyncException;
import org.apache.doris.stack.exception.NoPermissionException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import org.apache.doris.stack.exception.SqlSyntaxException;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
@Slf4j
public class DataManageService extends BaseService {
    @Autowired
    private ClusterInfoRepository clusterInfoRepository;

    @Autowired
    private NativeQueryService nativeQueryService;

    @Autowired
    private DorisDataBuildDriver driver;

    @Autowired
    private ManagerMetaSyncComponent syncComponent;

    @Autowired
    private PaloMetaInfoClient metaInfoClient;

    @Autowired
    private CoreUserRepository userRepository;

    @Autowired
    private ClusterUserComponent clusterUserComponent;

    @Autowired
    private DatabuildComponent databuildComponent;

    /**
     * create database
     * @param nsId
     * @param createDbInfo
     * @param studioUserId
     * @throws Exception
     */
    @Transactional
    public int createDatabse(int nsId, DbCreateReq createDbInfo, int studioUserId) throws Exception {
        log.debug("User {} create database {}.", studioUserId, createDbInfo.getName());
        checkRequestBody(createDbInfo.hasEmptyField());

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);

        String sql = driver.createDb(createDbInfo.getName());
        log.info("Create database,execute sql {}.", sql);
        // TODO:When creating a database, the database name is not required for executing SQL statements.
        //  It is set as the default metadata DB to avoid error reporting. In the future,
        //  Doris optimization interface is required
        nativeQueryService.executeSql(nsId, 0, sql, studioUserId);
        log.info("Create database success, save metadata.");

        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();
        DataDescription description = new DataDescription(createDbInfo.getDescribe(), userEntity.getFirstName());

        try {
            int dbId = syncComponent.addDatabase(createDbInfo.getName(), JSON.toJSONString(description),
                    nsId, clusterInfo.getId());
            log.info("save metadata db {} success.", dbId);
            return dbId;
        } catch (Exception e) {
            // If the metadata storage fails, it indicates that it has been successfully created in the engine.
            // If you want to roll back, you need to delete the data in the engine
            log.error("store database metadata exception,delete db in doris.");
            String deleteSql = "DROP DATABASE " + createDbInfo.getName();
            nativeQueryService.executeSql(deleteSql, createDbInfo.getName(), studioUserId);
            log.info("Delete database success.");
            throw e;
        }
    }

    @Transactional
    public void deleteDatabse(int nsId, int dbId, int studioUserId) throws Exception {
        if (dbId == ConstantDef.MYSQL_SCHEMA_DB_ID) {
            log.error("No permission to delete information_schema database.");
            throw new NoPermissionException();
        }
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);

        ManagerDatabaseEntity databaseEntity = databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());
        String sql = "DROP DATABASE " + databaseEntity.getName();
        log.info("Delete database sql {}.", sql);
        nativeQueryService.executeSql(nsId, dbId, sql, studioUserId);
        log.info("Delete database success, delete metadata");

        syncComponent.deleteDatabase(clusterInfo.getId(), dbId);
        log.info("Delete database {} metadata success.", dbId);
    }

    @Transactional
    public int createTable(int nsId, int dbId, TableCreateReq createTableInfo, int studioUserId) throws Exception {
        log.debug("User {} create table {} in database {}.", studioUserId, createTableInfo.getName(), dbId);

        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerDatabaseEntity databaseEntity = databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());

        String sql = driver.createTable(createTableInfo);
        log.info("Create table in db {}, execute sql {}.", databaseEntity.getName(), sql);
        nativeQueryService.executeSql(nsId, dbId, sql, studioUserId);

        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();
        DataDescription description = new DataDescription(createTableInfo.getDescribe(), userEntity.getFirstName());

        try {
            int tableId = saveTableMetadata(dbId, createTableInfo.getName(),
                    clusterInfo, databaseEntity.getName(), JSON.toJSONString(description));

            log.info("save metadata table {} success.", tableId);
            return tableId;
        } catch (Exception e) {
            log.error("store table metadata exception,delete table in doris.");
            String deleteSql = "DROP TABLE " + createTableInfo.getName();
            nativeQueryService.executeSql(nsId, dbId, deleteSql, studioUserId);
            log.info("Delete table success.");
            throw e;
        }
    }

    public String createTableSql(int nsId, int dbId, TableCreateReq createTableInfo) throws Exception {
        String sql = driver.createTable(createTableInfo);
        return sql;
    }

    @Transactional
    public int crateTableBySql(int nsId, int dbId, String sql, int studioUserId) throws Exception {
        if (StringUtils.isEmpty(sql)) {
            log.error("The query sql is empty");
            throw new RequestFieldNullException();
        }
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        ManagerDatabaseEntity databaseEntity = databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());

        CoreUserEntity userEntity = userRepository.findById(studioUserId).get();
        DataDescription description = new DataDescription("create by sql.", userEntity.getFirstName());
        log.info("User {} Create table,execute sql {}.", userEntity.getFirstName(), sql);
        String tableName = parseCreateTableSqlGetName(sql);
        nativeQueryService.executeSql(nsId, dbId, sql, studioUserId);
        try {
            int id = saveTableMetadata(dbId, tableName, clusterInfo,
                    databaseEntity.getName(), JSON.toJSONString(description));
            return id;
        } catch (Exception e) {
            log.error("Save meta data error.");
            e.printStackTrace();
            throw new MetaDataSyncException("Create table success, Save meta data error.");
        }
    }

    private int saveTableMetadata(int dbId, String tableName, ClusterInfoEntity clusterInfo, String dbName,
                                  String description) throws Exception {
        log.info("Create table success, save table metadata.");
        TableSchemaInfo.TableSchema tableSchema =
                metaInfoClient.getTableBaseSchema(ConstantDef.DORIS_DEFAULT_NS, dbName,
                        tableName, clusterInfo);
        int tableId = syncComponent.addTable(dbId, tableName, description, tableSchema);
        syncComponent.addTableFieldList(tableId, tableSchema);
        log.info("Sava table {} metadata success.", tableId);
        return tableId;
    }

    private String parseCreateTableSqlGetName(String createTableSql) throws Exception {
        String[] tempArray = createTableSql.split("\\(", 2);
        if (tempArray.length != 2) {
            throw new SqlSyntaxException("The create table sql is error.");
        }
        int startIndex = "CREATE TABLE ".length();

        String tableName = tempArray[0].substring(startIndex);
        tableName = tableName.trim();
        tableName = tableName.replaceAll("`", "");
        tableName = tableName.replaceAll("\"", "");
        return tableName;
    }
}

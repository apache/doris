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

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.response.construct.NativeQueryResp;
import org.apache.doris.stack.component.ClusterUserComponent;
import org.apache.doris.stack.component.DatabuildComponent;
import org.apache.doris.stack.connector.PaloQueryClient;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.entity.ManagerDatabaseEntity;
import org.apache.doris.stack.service.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class NativeQueryService extends BaseService {

    private PaloQueryClient paloQueryClient;

    private ClusterUserComponent clusterUserComponent;

    private DatabuildComponent databuildComponent;

    @Autowired
    public NativeQueryService(PaloQueryClient paloQueryClient,
                              ClusterUserComponent clusterUserComponent,
                              DatabuildComponent databuildComponent) {
        this.clusterUserComponent = clusterUserComponent;
        this.paloQueryClient = paloQueryClient;
        this.databuildComponent = databuildComponent;
    }

    /**
     * Implement SQL query through Doris HTTP protocol
     * @param nsId
     * @param dbId
     * @param sql
     * @param studioUserId
     * @return
     * @throws Exception
     */
    public NativeQueryResp executeSql(int nsId, int dbId, String sql, int studioUserId) throws Exception {
        log.debug("user {} execute sql {} in db {}", studioUserId, sql, dbId);
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
       String dbName = null;
        if (dbId < 1) {
            dbName = ConstantDef.MYSQL_DEFAULT_SCHEMA;
        } else {
            ManagerDatabaseEntity databaseEntity = databuildComponent.checkClusterDatabase(dbId, clusterInfo.getId());
            dbName = databaseEntity.getName();
        }
        return paloQueryClient.executeSQL(sql, ConstantDef.DORIS_DEFAULT_NS, dbName, clusterInfo);
    }

    /**
     * Execute SQL statement
     * @param sql
     * @param dbName
     * @param studioUserId
     * @return
     * @throws Exception
     */
    public NativeQueryResp executeSql(String sql, String dbName, int studioUserId) throws Exception {
        ClusterInfoEntity clusterInfo = clusterUserComponent.getClusterByUserId(studioUserId);
        return paloQueryClient.executeSQL(sql, ConstantDef.DORIS_DEFAULT_NS, dbName, clusterInfo);
    }
}

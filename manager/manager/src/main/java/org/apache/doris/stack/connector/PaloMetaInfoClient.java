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

package org.apache.doris.stack.connector;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.model.palo.TableSchemaInfo;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.exception.PaloRequestException;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PaloMetaInfoClient extends PaloClient {
    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloMetaInfoClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    public List<String> getDatabaseList(String nsName, ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/rest/v2/api/meta/");
        buffer.append(NAMESPACE);
        buffer.append("/");
        buffer.append(nsName);
        buffer.append("/");
        buffer.append(DATABASE);
        url = buffer.toString();

        log.debug("Send get database list request, url is {}.", url);
        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());
        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get Database list by ns error.");
        }
        List<String> result = JSON.parseArray(response.getData(), String.class);
        for (int i = 0; i < result.size(); i++) {
            String databaseNameTemp = result.get(i);
            String[] nameArray = databaseNameTemp.split(":");
            if (nameArray.length != 2) {
                throw new PaloRequestException("Get Database name format error:" + response.getData());
            }
            result.set(i, nameArray[1]);
        }

        return result;
    }

    public List<String> getTableList(String nsName, String dbName, ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/rest/v2/api/meta/");
        buffer.append(NAMESPACE);
        buffer.append("/");
        buffer.append(nsName);
        buffer.append("/");
        buffer.append(DATABASE);
        buffer.append("/");
        buffer.append(dbName);
        buffer.append("/");
        buffer.append(TABLE);
        url = buffer.toString();

        log.debug("Send get table list request, url is {}.", url);
        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get Table list by ns and db error:" + response.getData());
        }
        return JSON.parseArray(response.getData(), String.class);
    }

    public TableSchemaInfo.TableSchema getTableBaseSchema(String ns, String db, String table,
                                          ClusterInfoEntity entity) throws Exception {
        TableSchemaInfo result = getTableSchema(ns, db, table, entity);
        TableSchemaInfo.TableSchema tableSchema = result.getSchemaInfo().getSchemaMap().get(table);
        return tableSchema;
    }

    public TableSchemaInfo getTableSchema(String ns, String db, String table,
                                          ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/rest/v2/api/meta/");
        buffer.append(NAMESPACE);
        buffer.append("/");
        buffer.append(ns);
        buffer.append("/");
        buffer.append(DATABASE);
        buffer.append("/");
        buffer.append(db);
        buffer.append("/");
        buffer.append(TABLE);
        buffer.append("/");
        buffer.append(table);
        buffer.append("/schema");

        url = buffer.toString();
        log.debug("Send get table schema request, url is {}.", url);
        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        PaloResponseEntity response = poolManager.doGet(url, headers);
        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            throw new PaloRequestException("Get Table list by ns and db error:" + response.getData());
        }

        return JSON.parseObject(response.getData(), TableSchemaInfo.class);
    }
}

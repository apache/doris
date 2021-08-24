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
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.stack.model.palo.PaloResponseEntity;
import org.apache.doris.stack.model.response.construct.NativeQueryResp;
import org.apache.doris.stack.entity.ClusterInfoEntity;
import org.apache.doris.stack.exception.PaloRequestException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PaloQueryClient extends PaloClient {
    protected HttpClientPoolManager poolManager;

    @Autowired
    public PaloQueryClient(HttpClientPoolManager poolManager) {
        this.poolManager = poolManager;
    }

    /**
     * Create an ordinary analyst Palo large account user
     * @param ns
     * @param db
     * @param entity
     * @return
     */
    public String createUser(String ns, String db, ClusterInfoEntity entity, String userName) {
        String passwd = "123456";

        try {
            String createSql = "create user '" + userName + "'@'%' IDENTIFIED BY '" + passwd + "'";
            executeSQL(createSql, ns, db, entity);

            String permissionSql = "GRANT SELECT_PRIV ON *.* TO '" + userName + "'@'%'";
            executeSQL(permissionSql, ns, db, entity);
        } catch (Exception e) {
            log.warn("create user exception {}.", e.getMessage());
            e.printStackTrace();
        }

        return passwd;
    }

    public void deleteUser(String ns, String db, ClusterInfoEntity entity, String userName) {
        try {
            String deleteSql = "DROP USER '" + userName + "'@'%'";
            executeSQL(deleteSql, ns, db, entity);
        } catch (Exception e) {
            log.warn("delete user exception {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    public double countSQL(String sql, String ns, String db, ClusterInfoEntity entity) {
        try {
            NativeQueryResp resp = executeSQL(sql, ns, db, entity);
            List<List<String>> data = resp.getData();
            if (data.get(0).get(0) == null) {
                return 0;
            } else {
                return Double.parseDouble(data.get(0).get(0));
            }
        } catch (Exception e) {
            log.error("execute count sql {} error {}", sql, e.getMessage());
            e.printStackTrace();
            return 0.0;
        }
    }

    public NativeQueryResp executeSQL(String sql, String ns, String db, ClusterInfoEntity entity) throws Exception {
        String url = getHostUrl(entity.getAddress(), entity.getHttpPort());
        StringBuffer buffer = new StringBuffer();
        buffer.append(url);
        buffer.append("/api/query/");
        buffer.append(ns);
        buffer.append("/");
        buffer.append(db);
        url = buffer.toString();
        log.debug("Send execute sql {} request, url is {}.", sql, url);

        Map<String, String> headers = Maps.newHashMap();
        setHeaders(headers);
        setAuthHeaders(headers, entity.getUser(), entity.getPasswd());

        Map<String, String> responseBody = Maps.newHashMap();
        responseBody.put("stmt", sql);

        PaloResponseEntity response = poolManager.doPost(url, headers, responseBody);

        if (response.getCode() != REQUEST_SUCCESS_CODE) {
            // SQL execution error
            throw new PaloRequestException("Query palo error:" + response.getData());
        }

        JSONObject jsonObject = JSON.parseObject(response.getData());
        NativeQueryResp resp = new NativeQueryResp();
        String type = jsonObject.getString("type");
        resp.setType(type);
        int time = jsonObject.getInteger("time");
        resp.setTime(time);

        if (type.equals(NativeQueryResp.Type.result_set.name())) {
            List<List<String>> dataResult = Lists.newArrayList();
            String dataStr = jsonObject.getString("data");
            List<String> dataList = JSON.parseArray(dataStr, String.class);
            for (String data : dataList) {
                List<String> dataInfo = JSON.parseArray(data, String.class);
                dataResult.add(dataInfo);
            }
            resp.setData(dataResult);

            String metaStr = jsonObject.getString("meta");
            List<NativeQueryResp.Meta> metaList = JSON.parseArray(metaStr, NativeQueryResp.Meta.class);
            resp.setMeta(metaList);
        }
        return resp;
    }
}

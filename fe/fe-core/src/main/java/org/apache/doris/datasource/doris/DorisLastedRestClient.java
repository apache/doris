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

package org.apache.doris.datasource.doris;


import org.apache.doris.catalog.Column;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.rest.HealthAction;
import org.apache.doris.httpv2.rest.RestApiStatusCode;
import org.apache.doris.httpv2.rest.response.GsonSchemaResponse;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class DorisLastedRestClient extends DorisRestClient {
    /**
     * For DorisTable.
     */
    public DorisLastedRestClient(List<String> feNodes, String authUser, String authPassword, boolean httpSslEnable) {
        super(feNodes, authUser, authPassword, httpSslEnable);
    }

    @Override
    public List<String> getDatabaseNameList() {
        ResponseBody<ArrayList<String>> databasesResponse = parseResponse(
            new TypeToken<ArrayList<String>>() {},
                execute("api/meta/namespaces/default_cluster/databases"),
                    "get doris databases error");
        if (successResponse(databasesResponse)) {
            return databasesResponse.getData();
        }
        return new ArrayList<>();
    }

    @Override
    public List<String> getTablesNameList(String dbName) {
        ResponseBody<ArrayList<String>> tablesResponse = parseResponse(
            new TypeToken<ArrayList<String>>() {},
                execute("api/meta/namespaces/default_cluster/databases/" + dbName + "/tables"),
                "get doris tables error");
        if (successResponse(tablesResponse)) {
            return tablesResponse.getData();
        }
        return new ArrayList<>();
    }

    @Override
    public boolean isTableExist(String dbName, String tableName) {
        ResponseBody<GsonSchemaResponse> getColumnsResponse = parseResponse(
            new TypeToken<GsonSchemaResponse>(){},
                execute("api/" + dbName + "/" + tableName + "/_schema"),
                "get doris table schema error");
        return successResponse(getColumnsResponse);
    }

    @Override
    public boolean health() {
        ResponseBody<HashMap<String, Integer>> healthResponse = parseResponse(
            new TypeToken<HashMap<String, Integer>>() {},
                execute("api/health"),
                "get doris table schema error");
        int aliveBeNum = healthResponse.getData().get(HealthAction.ONLINE_BACKEND_NUM);
        return aliveBeNum > 0;
    }

    @Override
    public List<Column> getColumns(String dbName, String tableName) {
        ResponseBody<GsonSchemaResponse> getColumnsResponse = parseResponse(
            new TypeToken<GsonSchemaResponse>(){},
                execute("api/" + dbName + "/" + tableName + "/_gson_schema"),
                "get doris table schema error");
        if (successResponse(getColumnsResponse)) {
            return getColumnsResponse.getData().getJsonColumns().stream()
                .map(json -> GsonUtils.GSON.fromJson(json, Column.class))
                .collect(Collectors.toList());
        }
        throw new RuntimeException("get doris table schema error, msg: " + getColumnsResponse.getMsg());
    }

    private static <T> ResponseBody<T> parseResponse(TypeToken<T> typeToken, String responseBody, String errMsg) {
        if (responseBody == null) {
            throw new RuntimeException(errMsg);
        }

        Type type = TypeToken.getParameterized(ResponseBody.class, typeToken.getType()).getType();
        return GsonUtils.GSON.fromJson(responseBody, type);
    }

    private boolean successResponse(ResponseBody<?> responseBody) {
        return responseBody != null && responseBody.getCode() == RestApiStatusCode.OK.code;
    }
}

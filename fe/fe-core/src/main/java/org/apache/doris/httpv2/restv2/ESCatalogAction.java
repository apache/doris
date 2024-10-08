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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.es.EsExternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v2/api/es_catalog")
public class ESCatalogAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(ESCatalogAction.class);
    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";

    private Object handleRequest(HttpServletRequest request, HttpServletResponse response,
            BiFunction<EsExternalCatalog, String, String> action) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        Map<String, Object> resultMap = Maps.newHashMap();
        Env env = Env.getCurrentEnv();
        String catalogName = request.getParameter(CATALOG);
        String tableName = request.getParameter(TABLE);
        CatalogIf catalog = env.getCatalogMgr().getCatalog(catalogName);
        if (!(catalog instanceof EsExternalCatalog)) {
            return ResponseEntityBuilder.badRequest("unknown ES Catalog: " + catalogName);
        }
        EsExternalCatalog esExternalCatalog = (EsExternalCatalog) catalog;
        esExternalCatalog.makeSureInitialized();
        String result = action.apply(esExternalCatalog, tableName);
        ObjectNode jsonResult = JsonUtil.parseObject(result);

        resultMap.put("catalog", catalogName);
        resultMap.put("table", tableName);
        resultMap.put("result", jsonResult);

        return ResponseEntityBuilder.ok(resultMap);
    }

    @RequestMapping(path = "/get_mapping", method = RequestMethod.GET)
    public Object getMapping(HttpServletRequest request, HttpServletResponse response) {
        return handleRequest(request, response, (esExternalCatalog, tableName) ->
            esExternalCatalog.getEsRestClient().getMapping(tableName));
    }

    @RequestMapping(path = "/search", method = RequestMethod.POST)
    public Object search(HttpServletRequest request, HttpServletResponse response) {
        String body;
        try {
            body = getRequestBody(request);
        } catch (IOException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
        return handleRequest(request, response, (esExternalCatalog, tableName) ->
            esExternalCatalog.getEsRestClient().searchIndex(tableName, body));
    }

    private String getRequestBody(HttpServletRequest request) throws IOException {
        BufferedReader reader = request.getReader();
        return reader.lines().collect(Collectors.joining(System.lineSeparator()));
    }
}

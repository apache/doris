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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class is used to get query stats or clear query stats.
 */
@RestController
public class QueryStatsAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(QueryStatsAction.class);

    @RequestMapping(path = "/api/query_stats/{catalog}", method = RequestMethod.GET)
    protected Object getQueryStatsFromCatalog(@PathVariable("catalog") String catalog,
            @RequestParam(name = "summary", required = false, defaultValue = "true") boolean summary,
            @RequestParam(name = "pretty", required = false, defaultValue = "false") boolean pretty,
            HttpServletRequest request, HttpServletResponse response) {
        if (pretty && summary) {
            return ResponseEntityBuilder.badRequest("pretty and summary can not be true at the same time");
        }
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        try {
            Map<String, Map> result = Env.getCurrentEnv().getQueryStats().getStats(catalog, summary);
            if (pretty) {
                return ResponseEntityBuilder.ok(getPrettyJson(result.get("detail"), QueryStatsType.DATABASE));
            }
            return ResponseEntityBuilder.ok(result);
        } catch (Exception e) {
            LOG.warn("get query stats from catalog {} failed", catalog, e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/query_stats/{catalog}/{database}", method = RequestMethod.GET)
    protected Object getQueryStatsFromDatabase(@PathVariable("catalog") String catalog,
            @PathVariable("database") String database,
            @RequestParam(name = "summary", required = false, defaultValue = "true") boolean summary,
            @RequestParam(name = "pretty", required = false, defaultValue = "false") boolean pretty,
            HttpServletRequest request, HttpServletResponse response) {
        if (pretty && summary) {
            return ResponseEntityBuilder.badRequest("pretty and summary can not be true at the same time");
        }
        executeCheckPassword(request, response);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), database, PrivPredicate.SHOW);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        try {
            Map<String, Map> result = Env.getCurrentEnv().getQueryStats().getStats(catalog, database, summary);
            if (pretty) {
                return ResponseEntityBuilder.ok(getPrettyJson(result.get("detail"), QueryStatsType.TABLE));
            }
            return ResponseEntityBuilder.ok(result);
        } catch (Exception e) {
            LOG.warn("get query stats from catalog {} failed", catalog, e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/query_stats/{catalog}/{database}/{table}", method = RequestMethod.GET)
    protected Object getQueryStatsFromTable(@PathVariable("catalog") String catalog,
            @PathVariable("database") String database, @PathVariable("table") String table,
            @RequestParam(name = "summary", required = false, defaultValue = "true") boolean summary,
            @RequestParam(name = "pretty", required = false, defaultValue = "false") boolean pretty,
            HttpServletRequest request, HttpServletResponse response) {
        if (pretty && summary) {
            return ResponseEntityBuilder.badRequest("pretty and summary can not be true at the same time");
        }
        executeCheckPassword(request, response);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), database, table, PrivPredicate.SHOW);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        try {
            Map<String, Map> result = Env.getCurrentEnv().getQueryStats().getStats(catalog, database, table, summary);
            if (pretty) {
                return ResponseEntityBuilder.ok(getPrettyJson(result.get("detail"), QueryStatsType.INDEX));
            }
            return ResponseEntityBuilder.ok(result);
        } catch (Exception e) {
            LOG.warn("get query stats from catalog {} failed", catalog, e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/query_stats/{catalog}/{database}/{table}/{index}", method = RequestMethod.GET)
    protected Object getQueryStatsFromIndex(@PathVariable("catalog") String catalog,
            @PathVariable("database") String database, @PathVariable("table") String table,
            @PathVariable("index") String index,
            @RequestParam(name = "summary", required = false, defaultValue = "true") boolean summary,
            @RequestParam(name = "pretty", required = false, defaultValue = "false") boolean pretty,
            HttpServletRequest request, HttpServletResponse response) {
        if (pretty && summary) {
            return ResponseEntityBuilder.badRequest("pretty and summary can not be true at the same time");
        }
        executeCheckPassword(request, response);
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), database, table, PrivPredicate.SHOW);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        try {
            Map<String, Map> result = Env.getCurrentEnv().getQueryStats()
                    .getStats(catalog, database, table, index, summary);
            if (pretty) {
                return ResponseEntityBuilder.ok(getPrettyJson(result.get("detail"), QueryStatsType.COLUMN));
            }
            return ResponseEntityBuilder.ok(result);
        } catch (Exception e) {
            LOG.warn("get query stats from catalog {} failed", catalog, e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/query_stats/{catalog}/{database}", method = RequestMethod.DELETE)
    protected Object clearQueryStatsFromDatabase(@PathVariable("catalog") String catalog,
            @PathVariable("database") String database, HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        Env.getCurrentEnv().getQueryStats().clear(catalog, database);
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/query_stats/{catalog}/{database}/{table}", method = RequestMethod.DELETE)
    protected Object clearQueryStatsFromTable(@PathVariable("catalog") String catalog,
            @PathVariable("database") String database, @PathVariable("table") String table, HttpServletRequest request,
            HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        // use NS_KEY as catalog, but NS_KEY's default value is 'default_cluster'.
        if (catalog.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
        }
        Env.getCurrentEnv().getQueryStats().clear(catalog, database, table);
        return ResponseEntityBuilder.ok();
    }

    private JSONArray getPrettyJson(Map<String, Map> stats, QueryStatsType type) {
        JSONArray result = new JSONArray();
        QueryStatsType nextType = QueryStatsType.INVALID;
        switch (type) {
            case CATALOG:
                nextType = QueryStatsType.DATABASE;
                break;
            case DATABASE:
                nextType = QueryStatsType.TABLE;
                break;
            case TABLE:
                nextType = QueryStatsType.INDEX;
                break;
            case INDEX:
                nextType = QueryStatsType.COLUMN;
                break;
            case COLUMN:
                nextType = QueryStatsType.DETAIL;
                break;
            default:
                break;
        }
        for (Map.Entry<String, Map> entry : stats.entrySet()) {
            JSONObject obj = new JSONObject();
            if (type == QueryStatsType.COLUMN) {
                obj.put("name", entry.getKey());
                obj.put("type", type.toString());
                Map<String, Long> columnStats = (Map) entry.getValue();
                obj.put("value",
                        Math.max(columnStats.getOrDefault("query", 0L), columnStats.getOrDefault("filter", 0L)));
                JSONArray children = new JSONArray();
                BiFunction<Map<String, Long>, Boolean, JSONObject> genDetail
                        = (Map<String, Long> detail, Boolean query) -> {
                            JSONObject detailObj = new JSONObject();
                            detailObj.put("type", "DETAIL");
                            if (query) {
                                detailObj.put("name", "query");
                                detailObj.put("value", detail.getOrDefault("query", 0L));
                            } else {
                                detailObj.put("name", "filter");
                                detailObj.put("value", detail.getOrDefault("filter", 0L));
                            }
                            return detailObj;
                        };
                children.add(genDetail.apply(columnStats, true));
                children.add(genDetail.apply(columnStats, false));
                obj.put("children", children);
            } else {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }
                if (type == QueryStatsType.DATABASE && entry.getKey().contains(":")) {
                    obj.put("name", entry.getKey().split(":")[1]);
                } else {
                    obj.put("name", entry.getKey());
                }
                obj.put("type", type.toString());
                Map<String, Long> summary = (Map) entry.getValue().get("summary");
                obj.put("value", summary.getOrDefault("query", 0L));
                if (entry.getValue().containsKey("detail") && nextType != QueryStatsType.INVALID) {
                    obj.put("children", getPrettyJson((Map) entry.getValue().get("detail"), nextType));
                }
            }
            result.add(obj);
        }
        return result;
    }

    enum QueryStatsType {
        CATALOG(1), DATABASE(2), TABLE(3), INDEX(4), COLUMN(5), DETAIL(6), INVALID(99);
        private static Map map = new HashMap<>();
        private int value;

        static {
            for (QueryStatsType queryStatsType : QueryStatsType.values()) {
                map.put(queryStatsType.value, queryStatsType);
            }
        }

        QueryStatsType(int i) {
            this.value = value;
        }

        public static QueryStatsType valueOf(int value) {
            QueryStatsType queryStatsType = (QueryStatsType) map.get(value);
            if (queryStatsType == null) {
                return QueryStatsType.INVALID;
            }
            return queryStatsType;
        }

        public int getValue() {
            return value;
        }
    }
}

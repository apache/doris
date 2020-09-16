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

package org.apache.doris.httpv2.controller;

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.httpv2.entity.ResponseBody;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class QueryProfileController extends BaseController {
    private static final Logger LOG = LogManager.getLogger(QueryProfileController.class);

    private static final String QUERY_ID = "query_id";

    @RequestMapping(path = "/query_profile/{" + QUERY_ID + "}", method = RequestMethod.GET)
    public Object profile(@PathVariable(value = QUERY_ID) String queryId) {
        String profile = ProfileManager.getInstance().getProfile(queryId);
        if (profile == null) {
            return ResponseEntityBuilder.okWithCommonError("Query " + queryId + " does not exist");
        }
        profile = profile.replaceAll("\n", "</br>");
        profile = profile.replaceAll(" ", "&nbsp;&nbsp;");
        return ResponseEntityBuilder.ok(profile);
    }

    @RequestMapping(path = "/query_profile", method = RequestMethod.GET)
    public Object query() {
        Map<String, Object> result = Maps.newHashMap();
        addFinishedQueryInfo(result);
        ResponseEntity entity = ResponseEntityBuilder.ok(result);
        ((ResponseBody) entity.getBody()).setCount(result.size());
        return entity;
    }

    private void addFinishedQueryInfo(Map<String, Object> result) {
        List<List<String>> finishedQueries = ProfileManager.getInstance().getAllQueries();
        List<String> columnHeaders = ProfileManager.PROFILE_HEADERS;
        int queryIdIndex = 0; // the first column is 'Query ID' by default
        for (int i = 0; i < columnHeaders.size(); ++i) {
            if (columnHeaders.get(i).equals(ProfileManager.QUERY_ID)) {
                queryIdIndex = i;
                break;
            }
        }

        result.put("column_names", columnHeaders);
        result.put("href_column", Lists.newArrayList(ProfileManager.QUERY_ID));
        List<Map<String, Object>> list = Lists.newArrayList();
        result.put("rows", list);

        for (List<String> row : finishedQueries) {
            String queryId = row.get(queryIdIndex);
            Map<String, Object> rowMap = new HashMap<>();
            for (int i = 0; i < row.size(); ++i) {
                rowMap.put(columnHeaders.get(i), row.get(i));
            }

            // add hyper link
            if (Strings.isNullOrEmpty(queryId)) {
                rowMap.put("__hrefPaths", Lists.newArrayList("/query_profile/-1"));
            } else {
                rowMap.put("__hrefPaths", Lists.newArrayList("/query_profile/" + queryId));
            }
            list.add(rowMap);
        }
    }
}

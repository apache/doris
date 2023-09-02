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

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v1")
public class ConfigController {
    private static final Logger LOG = LogManager.getLogger(ConfigController.class);
    private static final List<String> CONFIG_TABLE_HEADER = Lists.newArrayList("Name", "Value");
    private static final String CONF_ITEM = "conf_item";

    @RequestMapping(path = "/config/fe", method = RequestMethod.GET)
    public Object variable(HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> result = Maps.newHashMap();
        String confItem = request.getParameter(CONF_ITEM);
        appendConfigureInfo(result, confItem);
        return ResponseEntityBuilder.ok(result);
    }

    private void appendConfigureInfo(Map<String, Object> result, String confItem) {
        result.put("column_names", CONFIG_TABLE_HEADER);
        List<Map<String, String>> list = Lists.newArrayList();
        result.put("rows", list);
        try {
            Map<String, String> confmap = Config.dump();
            if (!Strings.isNullOrEmpty(confItem)) {
                Map<String, String> info = new HashMap<>();
                info.put("Name", confItem);
                info.put("Value", confmap.get(confItem));
                list.add(info);
            } else {
                for (String key : confmap.keySet()) {
                    Map<String, String> info = new HashMap<>();
                    info.put("Name", key);
                    info.put("Value", confmap.get(key));
                    list.add(info);
                }
            }
        } catch (Exception e) {
            LOG.warn("", e);
        }
    }
}

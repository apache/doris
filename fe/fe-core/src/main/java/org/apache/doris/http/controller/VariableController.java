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

package org.apache.doris.http.controller;

import org.apache.doris.analysis.SetType;
import org.apache.doris.common.Config;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.qe.VariableMgr;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class VariableController {

    @RequestMapping(path = "/variable", method = RequestMethod.GET)
    public Object variable() {
        Map<String, Object> result = new HashMap<>();
        appendConfigureInfo(result);
        appendVariableInfo(result);
        return ResponseEntityBuilder.ok(result);
    }

    private void appendConfigureInfo(Map<String, Object> result) {
        HashMap<String, String> confmap;
        List<Map<String, String>> confList = new ArrayList<>();
        try {
            confmap = Config.dump();
            for (String key : confmap.keySet()) {
                Map<String, String> info = new HashMap<>();
                info.put("Name", key);
                info.put("Value", confmap.get(key));
                confList.add(info);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        result.put("configureInfo", confList);
    }

    private void appendVariableInfo(Map<String, Object> result) {
        List<Map<String, String>> varList = new ArrayList<>();
        List<List<String>> variableInfo = VariableMgr.dump(SetType.GLOBAL, null, null);
        for (List<String> list : variableInfo) {
            Map<String, String> info = new HashMap<>();
            info.put("Name", list.get(0));
            info.put("Value", list.get(1));
            varList.add(info);
        }
        result.put("variableInfo", varList);
    }
}

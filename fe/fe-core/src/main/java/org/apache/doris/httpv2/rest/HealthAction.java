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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthAction extends RestBaseController {

    @RequestMapping(path = "/api/health", method = RequestMethod.GET)
    public Object execute() {
        Map<String, Object> result = new HashMap<>();
        result.put("total_backend_num", Catalog.getCurrentSystemInfo().getBackendIds(false).size());
        result.put("online_backend_num", Catalog.getCurrentSystemInfo().getBackendIds(true).size());
        return ResponseEntityBuilder.ok(result);
    }
}

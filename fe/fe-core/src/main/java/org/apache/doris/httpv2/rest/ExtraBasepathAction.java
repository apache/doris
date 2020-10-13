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

import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import org.apache.parquet.Strings;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Api for getting the base path of api
 * Base path is the URL prefix for all API paths.
 * Some deployment environments need to configure additional base path to match resources.
 * This Api will return the path configured in Config.http_api_extra_base_path.
 */
@RestController
public class ExtraBasepathAction {
    @RequestMapping(path = "/api/basepath", method = RequestMethod.GET)
    public ResponseEntity execute(HttpServletRequest request, HttpServletResponse response) {
        BasepathResponse resp = new BasepathResponse();
        resp.path = Config.http_api_extra_base_path;
        if (Strings.isNullOrEmpty(Config.http_api_extra_base_path)) {
            resp.enable = false;
        } else {
            resp.enable = true;
        }
        return ResponseEntityBuilder.ok(resp);
    }

    public static class BasepathResponse {
        public boolean enable; // enable is false mean no extra base path configured.
        public String path;

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }
}



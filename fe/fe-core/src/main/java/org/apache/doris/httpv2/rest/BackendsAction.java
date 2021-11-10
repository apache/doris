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
import org.apache.doris.system.Backend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.Getter;
import lombok.Setter;

/**
 * This class responsible for returning current backends info.
 * Mainly used for flink/spark connector, which need backends info to execute stream load.
 * It only requires password, no auth check.
 * <p>
 * Response:
 * <p>
 * {
 * "msg": "success",
 * "code": 0,
 * "data": {
 * "backends": [
 * {
 * "ip": "192.1.1.1",
 * "http_port": 8040,
 * "is_alive": true
 * }
 * ]
 * },
 * "count": 0
 * }
 */
@RestController
public class BackendsAction extends RestBaseController {
    public static final Logger LOG = LogManager.getLogger(BackendsAction.class);

    private static final String IS_ALIVE = "is_alive";

    @RequestMapping(path = "/api/backends", method = {RequestMethod.GET})
    public Object getBackends(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        boolean needAlive = false;
        String isAlive = request.getParameter(IS_ALIVE);
        if (!Strings.isNullOrEmpty(isAlive) && isAlive.equalsIgnoreCase("true")) {
            needAlive = true;
        }

        BackendInfo backendInfo = new BackendInfo();
        backendInfo.backends = Lists.newArrayList();
        List<Long> beIds = Catalog.getCurrentSystemInfo().getBackendIds(needAlive);
        for (Long beId : beIds) {
            Backend be = Catalog.getCurrentSystemInfo().getBackend(beId);
            if (be != null) {
                BackendRow backendRow = new BackendRow();
                backendRow.ip = be.getHost();
                backendRow.httpPort = be.getHttpPort();
                backendRow.isAlive = be.isAlive();
                backendInfo.backends.add(backendRow);
            }
        }
        return ResponseEntityBuilder.ok(backendInfo);
    }

    @Getter
    @Setter
    public static class BackendInfo {
        @JsonProperty("backends")
        public List<BackendRow> backends;
    }

    @Getter
    @Setter
    public static class BackendRow {
        @JsonProperty("ip")
        public String ip;
        @JsonProperty("http_port")
        public int httpPort;
        @JsonProperty("is_alive")
        public boolean isAlive;
    }
}

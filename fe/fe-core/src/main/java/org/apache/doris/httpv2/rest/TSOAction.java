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
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * TSOAction is used to get TSO (Timestamp Oracle) information via HTTP interface.
 * This interface only allows retrieving TSO information without increasing the TSO value.
 *
 * Example usage:
 * GET /api/tso
 * Response:
 * {
 *   "window_end_physical_time": 1625097600000,
 *   "current_tso": 123456789012345678,
 *   "current_tso_physical_time": 1625097600000,
 *   "current_tso_logical_counter": 123
 * }
 */
@RestController
public class TSOAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(TSOAction.class);

    /**
     * Get current TSO information.
     * This interface only returns TSO information without increasing the TSO value.
     *
     * @param request  HTTP request
     * @param response HTTP response
     * @return ResponseEntity with TSO information including:
     *         - window_end_physical_time: The end time of the current TSO window
     *         - current_tso: The current composed TSO timestamp
     *         - current_tso_physical_time: The physical time part of current TSO
     *         - current_tso_logical_counter: The logical counter part of current TSO
     */
    @RequestMapping(path = "/api/tso", method = RequestMethod.GET)
    public Object getTSO(HttpServletRequest request, HttpServletResponse response) {
        try {
            //check user auth
            executeCheckPassword(request, response);

            Env env = Env.getCurrentEnv();
            if (env == null || !env.isReady()) {
                LOG.warn("TSO HTTP API: FE is not ready");
                return ResponseEntityBuilder.badRequest("FE is not ready");
            }
            if (!env.isMaster()) {
                LOG.warn("TSO HTTP API: current FE is not master");
                return ResponseEntityBuilder.badRequest("Current FE is not master");
            }
            // Get current TSO information without increasing it
            long windowEndPhysicalTime = env.getTSOService().getWindowEndTSO();
            long currentTSO = env.getTSOService().getCurrentTSO();

            // Prepare response data with detailed TSO information
            Map<String, Object> result = Maps.newHashMap();
            result.put("window_end_physical_time", windowEndPhysicalTime);
            result.put("current_tso", currentTSO);
            result.put("current_tso_physical_time", TSOTimestamp.extractPhysicalTime(currentTSO));
            result.put("current_tso_logical_counter", TSOTimestamp.extractLogicalCounter(currentTSO));
            return ResponseEntityBuilder.ok(result);
        } catch (Exception e) {
            LOG.warn("Failed to get TSO information", e);
            return ResponseEntityBuilder.badRequest(e.getMessage());
        }
    }
}

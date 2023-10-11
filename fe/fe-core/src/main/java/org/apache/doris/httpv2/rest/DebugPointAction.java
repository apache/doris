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
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class is used to add, remove or clear debug points.
 */
@RestController
public class DebugPointAction extends RestBaseController {

    @RequestMapping(path = "/api/debug_point/add/{debugPoint}", method = RequestMethod.POST)
    protected Object addDebugPoint(@PathVariable("debugPoint") String debugPoint,
            @RequestParam(name = "execute", required = false, defaultValue = "") String execute,
            @RequestParam(name = "timeout", required = false, defaultValue = "") String timeout,
            HttpServletRequest request, HttpServletResponse response) {
        if (!Config.enable_debug_points) {
            return ResponseEntityBuilder.internalError(
                    "Disable debug points. please check Config.enable_debug_points");
        }
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        if (Strings.isNullOrEmpty(debugPoint)) {
            return ResponseEntityBuilder.badRequest("Empty debug point name.");
        }
        int executeLimit = -1;
        if (!Strings.isNullOrEmpty(execute)) {
            try {
                executeLimit = Integer.valueOf(execute);
            } catch (Exception e) {
                return ResponseEntityBuilder.badRequest(
                        "Invalid execute format: " + execute + ", err " + e.getMessage());
            }
        }
        long timeoutSeconds = -1;
        if (!Strings.isNullOrEmpty(timeout)) {
            try {
                timeoutSeconds = Long.valueOf(timeout);
            } catch (Exception e) {
                return ResponseEntityBuilder.badRequest(
                        "Invalid timeout format: " + timeout + ", err " + e.getMessage());
            }
        }
        DebugPointUtil.addDebugPoint(debugPoint, executeLimit, timeoutSeconds);
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/debug_point/remove/{debugPoint}", method = RequestMethod.POST)
    protected Object removeDebugPoint(@PathVariable("debugPoint") String debugPoint,
            HttpServletRequest request, HttpServletResponse response) {
        if (!Config.enable_debug_points) {
            return ResponseEntityBuilder.internalError(
                    "Disable debug points. please check Config.enable_debug_points");
        }
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        if (Strings.isNullOrEmpty(debugPoint)) {
            return ResponseEntityBuilder.badRequest("Empty debug point name.");
        }
        DebugPointUtil.removeDebugPoint(debugPoint);
        return ResponseEntityBuilder.ok();
    }

    @RequestMapping(path = "/api/debug_point/clear", method = RequestMethod.POST)
    protected Object clearDebugPoints(HttpServletRequest request, HttpServletResponse response) {
        if (!Config.enable_debug_points) {
            return ResponseEntityBuilder.internalError(
                    "Disable debug points. please check Config.enable_debug_points");
        }
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        DebugPointUtil.clearDebugPoints();
        return ResponseEntityBuilder.ok();
    }
}

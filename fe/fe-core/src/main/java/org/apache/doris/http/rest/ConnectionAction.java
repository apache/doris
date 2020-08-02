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

package org.apache.doris.http.rest;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.http.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Maps;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

// This class is used to get current query_id of connection_id.
// Every connection holds at most one query at every point.
// Some we can get query_id firstly, and get query by query_id.
@RestController
public class ConnectionAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ConnectionAction.class);

    @RequestMapping(path = "/api/connection",method = RequestMethod.GET)
    protected Object connection(HttpServletRequest request, HttpServletResponse response) throws UnauthorizedException {
        executeCheckPassword(request, response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String connStr = request.getParameter("connection_id");
        if (connStr == null) {
            entity.setMsgWithCode("Missing connection_id", RestApiStatusCode.COMMON_ERROR);
            return entity;
        }

        long connectionId = -1;
        try {
            connectionId = Long.valueOf(connStr.trim());
        } catch (NumberFormatException e) {
            entity.setMsgWithCode("Invalid connection id: " + e.getMessage(), RestApiStatusCode.COMMON_ERROR);
            return entity;
        }

        ConnectContext context = ExecuteEnv.getInstance().getScheduler().getContext(connectionId);
        if (context == null || context.queryId() == null) {
            entity.setMsgWithCode("connection id " + connectionId + " not found.", RestApiStatusCode.COMMON_ERROR);
            return entity;
        }
        String queryId = DebugUtil.printId(context.queryId());

        Map<String, String> result = Maps.newHashMap();
        result.put("query_id", queryId);
        entity.setData(result);
        return entity;
    }
}

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

import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.util.ExecutionResultSet;
import org.apache.doris.httpv2.util.StatementSubmitter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * For execute stmt via http
 */
@RestController
public class StmtExecutionAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(StmtExecutionAction.class);
    private static StatementSubmitter stmtSubmitter = new StatementSubmitter();

    private static final String PARAM_SYNC = "sync";
    private static final String PARAM_LIMIT = "limit";

    private static final long DEFAULT_ROW_LIMIT = 1000;
    private static final long MAX_ROW_LIMIT = 10000;

    /**
     * Execute a SQL.
     * Request body:
     * {
     *     "stmt" : "select * from tbl1"
     * }
     */
    @RequestMapping(path = "/api/query/{" + NS_KEY + "}/{" + DB_KEY + "}", method = {RequestMethod.POST})
    public Object executeSQL(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = DB_KEY) String dbName,
            HttpServletRequest request, HttpServletResponse response,
            @RequestBody String stmtBody) throws DdlException {
        ActionAuthorizationInfo authInfo = checkWithCookie(request, response, false);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        boolean isSync = true;
        String syncParam = request.getParameter(PARAM_SYNC);
        if (!Strings.isNullOrEmpty(syncParam)) {
            isSync = syncParam.equals("1");
        }

        String limitParam = request.getParameter(PARAM_LIMIT);
        long limit = DEFAULT_ROW_LIMIT;
        if (!Strings.isNullOrEmpty(limitParam)) {
            limit = Math.min(Long.valueOf(limitParam), MAX_ROW_LIMIT);
        }

        Type type = new TypeToken<StmtRequestBody>() {
        }.getType();
        StmtRequestBody stmtRequestBody = new Gson().fromJson(stmtBody, type);

        if (Strings.isNullOrEmpty(stmtRequestBody.stmt)) {
            return ResponseEntityBuilder.badRequest("Missing statement request body");
        }
        LOG.info("stmt: {}", stmtRequestBody.stmt);

        ConnectContext.get().setDatabase(getFullDbName(dbName));

        // 2. Submit stmt
        StatementSubmitter.StmtContext stmtCtx = new StatementSubmitter.StmtContext(
                stmtRequestBody.stmt, authInfo.fullUserName, authInfo.password, limit
        );
        Future<ExecutionResultSet> future = stmtSubmitter.submit(stmtCtx);

        if (isSync) {
            try {
                ExecutionResultSet resultSet = future.get();
                return ResponseEntityBuilder.ok(resultSet.getResult());
            } catch (InterruptedException e) {
                LOG.warn("failed to execute stmt", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            } catch (ExecutionException e) {
                LOG.warn("failed to execute stmt", e);
                return ResponseEntityBuilder.okWithCommonError("Failed to execute sql: " + e.getMessage());
            }
        } else {
            return ResponseEntityBuilder.okWithCommonError("Not support async query execution");
        }
    }

    private static class StmtRequestBody {
        public String stmt;
    }
}

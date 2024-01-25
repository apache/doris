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
import org.apache.doris.common.Config;
import org.apache.doris.common.Version;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Api for checking the whether the FE has been started successfully.
 * Response
 * {
 *   "msg": "OK",
 *   "code": 0,
 *   "data": {
 *     "queryPort": 9030,
 *     "rpcPort": 9020,
 *     "arrowFlightSqlPort": 9040,
 *     "maxReplayedJournal": 17287
 *    },
 *   "count": 0
 * }
 */
@RestController
public class BootstrapFinishAction extends RestBaseController {

    private static final String CLUSTER_ID = "cluster_id";
    private static final String TOKEN = "token";

    public static final String REPLAYED_JOURNAL_ID = "replayedJournalId";
    public static final String QUERY_PORT = "queryPort";
    public static final String ARROW_FLIGHT_SQL_PORT = "arrowFlightSqlPort";
    public static final String RPC_PORT = "rpcPort";
    public static final String VERSION = "version";

    @RequestMapping(path = "/api/bootstrap", method = RequestMethod.GET)
    public ResponseEntity execute(HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        boolean isReady = Env.getCurrentEnv().isReady();

        // to json response
        BootstrapResult result = new BootstrapResult();
        if (isReady) {
            String clusterIdStr = request.getParameter(CLUSTER_ID);
            String token = request.getParameter(TOKEN);
            if (!Strings.isNullOrEmpty(clusterIdStr) && !Strings.isNullOrEmpty(token)) {
                // cluster id or token is provided, return more info
                int clusterId = 0;
                try {
                    clusterId = Integer.valueOf(clusterIdStr);
                } catch (NumberFormatException e) {
                    return ResponseEntityBuilder.badRequest("invalid cluster id format: " + clusterIdStr);
                }

                if (clusterId != Env.getCurrentEnv().getClusterId()) {
                    return ResponseEntityBuilder.okWithCommonError("invalid cluster id: " + clusterId);
                }

                if (!token.equals(Env.getCurrentEnv().getToken())) {
                    return ResponseEntityBuilder.okWithCommonError("invalid token: " + token);
                }

                // cluster id and token are valid, return replayed journal id
                long replayedJournalId = Env.getCurrentEnv().getReplayedJournalId();
                result.setReplayedJournalId(replayedJournalId);
                result.setQueryPort(Config.query_port);
                result.setRpcPort(Config.rpc_port);
                result.setArrowFlightSqlPort(Config.arrow_flight_sql_port);
                result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
            }

            return ResponseEntityBuilder.ok(result);
        }

        return ResponseEntityBuilder.okWithCommonError("not ready");
    }

    /**
     * This class is also for json DeSer, so get/set method must be remained.
     */
    private static class BootstrapResult {
        private long replayedJournalId = 0;
        private int queryPort = 0;
        private int rpcPort = 0;
        private int arrowFlightSqlPort = 0;
        private String version = "";

        public BootstrapResult() {

        }

        public void setReplayedJournalId(long replayedJournalId) {
            this.replayedJournalId = replayedJournalId;
        }

        public long getReplayedJournalId() {
            return replayedJournalId;
        }

        public void setQueryPort(int queryPort) {
            this.queryPort = queryPort;
        }

        public void setArrowFlightSqlPort(int arrowFlightSqlPort) {
            this.arrowFlightSqlPort = arrowFlightSqlPort;
        }

        public int getQueryPort() {
            return queryPort;
        }

        public int getArrowFlightSqlPort() {
            return arrowFlightSqlPort;
        }

        public void setRpcPort(int rpcPort) {
            this.rpcPort = rpcPort;
        }

        public int getRpcPort() {
            return rpcPort;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

    }
}

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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class BootstrapFinishAction {

    private static final String CLUSTER_ID = "cluster_id";
    private static final String TOKEN = "token";

    public static final String REPLAYED_JOURNAL_ID = "replayedJournalId";
    public static final String QUERY_PORT = "queryPort";
    public static final String RPC_PORT = "rpcPort";


    @RequestMapping(path = "/api/bootstrap",method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        boolean isReady = Catalog.getCurrentCatalog().isReady();

        // to json response
        BootstrapResult result = null;
        if (isReady) {
            result = new BootstrapResult();
            String clusterIdStr = request.getParameter(CLUSTER_ID);
            String token = request.getParameter(TOKEN);
            if (!Strings.isNullOrEmpty(clusterIdStr) && !Strings.isNullOrEmpty(token)) {
                // cluster id or token is provided, return more info
                int clusterId = 0;
                try {
                    clusterId = Integer.valueOf(clusterIdStr);
                } catch (NumberFormatException e) {
                    result.status = ActionStatus.FAILED;
                    result.msg = "invalid cluster id format: " + clusterIdStr;
                }

                if (result.status == ActionStatus.OK) {
                    if (clusterId != Catalog.getCurrentCatalog().getClusterId()) {
                        result.status = ActionStatus.FAILED;
                        result.msg = "invalid cluster id: " + Catalog.getCurrentCatalog().getClusterId();
                    }
                }

                if (result.status == ActionStatus.OK) {
                    if (!token.equals(Catalog.getCurrentCatalog().getToken())) {
                        result.status = ActionStatus.FAILED;
                        result.msg = "invalid token: " + Catalog.getCurrentCatalog().getToken();
                    }
                }

                if (result.status == ActionStatus.OK) {
                    // cluster id and token are valid, return replayed journal id
                    long replayedJournalId = Catalog.getCurrentCatalog().getReplayedJournalId();
                    result.setMaxReplayedJournal(replayedJournalId);
                    result.setQueryPort(Config.query_port);
                    result.setRpcPort(Config.rpc_port);
                }
            }
        } else {
            result = new BootstrapResult("not ready");
        }

        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(result);
        return entity;
    }

    public static class BootstrapResult extends RestBaseResult {
        private long replayedJournalId = 0;
        private int queryPort = 0;
        private int rpcPort = 0;

        public BootstrapResult() {
            super();
        }

        public BootstrapResult(String msg) {
            super(msg);
        }

        public void setMaxReplayedJournal(long replayedJournalId) {
            this.replayedJournalId = replayedJournalId;
        }

        public long getMaxReplayedJournal() {
            return replayedJournalId;
        }

        public void setQueryPort(int queryPort) {
            this.queryPort = queryPort;
        }

        public int getQueryPort() {
            return queryPort;
        }

        public void setRpcPort(int rpcPort) {
            this.rpcPort = rpcPort;
        }

        public int getRpcPort() {
            return rpcPort;
        }
        @Override
        public String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }
}

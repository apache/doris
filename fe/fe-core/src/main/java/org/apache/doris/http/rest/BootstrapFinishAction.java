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
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;
import com.google.gson.Gson;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.doris.common.Version;

/*
 * fe_host:fe_http_port/api/bootstrap
 * return:
 * {"status":"OK","msg":"Success","replayedJournal"=123456, "queryPort"=9000, "rpcPort"=9001}
 * {"status":"FAILED","msg":"err info..."}
 */
public class BootstrapFinishAction extends RestBaseAction {
    private static final String CLUSTER_ID = "cluster_id";
    private static final String TOKEN = "token";

    public static final String REPLAYED_JOURNAL_ID = "replayedJournalId";
    public static final String QUERY_PORT = "queryPort";
    public static final String RPC_PORT = "rpcPort";
    public static final String VERSION = "version";

    public BootstrapFinishAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/bootstrap", new BootstrapFinishAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        boolean isReady = Catalog.getCurrentCatalog().isReady();

        // to json response
        BootstrapResult result = null;
        if (isReady) {
            result = new BootstrapResult();
            String clusterIdStr = request.getSingleParameter(CLUSTER_ID);
            String token = request.getSingleParameter(TOKEN);
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
                    result.setVersion(Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
                }
            }
        } else {
            result = new BootstrapResult("not ready");
        }

        // send result
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response);
    }

    public static class BootstrapResult extends RestBaseResult {
        private long replayedJournalId = 0;
        private int queryPort = 0;
        private int rpcPort = 0;
        private String version = "";

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

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        @Override
        public String toJson() {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }
}

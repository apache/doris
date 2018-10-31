// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.rest;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

public class GetStreamLoadState extends RestBaseAction {
    private static final String DB_KEY = "db";
    private static final String LABEL_KEY = "label";

    public GetStreamLoadState(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        GetStreamLoadState action = new GetStreamLoadState(controller);
        controller.registerHandler(HttpMethod.GET, "/api/{" + DB_KEY + "}/{" + LABEL_KEY + "}/_state", action);
    }

    @Override
    public void executeWithoutPassword(AuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
            throws DdlException {

        if (redirectToMaster(request, response)) {
            return;
        }

        final String clusterName = authInfo.cluster;
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new DdlException("No cluster selected.");
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

        String label = request.getSingleParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected.");
        }

        // FIXME(cmy)
        // checkReadPriv(authInfo.fullUserName, fullDbName);

        Database db = Catalog.getInstance().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("unknown database, database=" + dbName);
        }

        String state = Catalog.getCurrentGlobalTransactionMgr().getLabelState(db.getId(), label).toString();

        sendResult(request, response, new Result(state));
    }

    private static class Result extends RestBaseResult {
        private String state;
        public Result(String state) {
            this.state = state;
        }
    }
}

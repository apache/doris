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

import java.util.Map;

import com.baidu.palo.analysis.LoadStmt;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.IllegalArgException;
import com.baidu.palo.service.ExecuteEnv;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import io.netty.handler.codec.http.HttpMethod;

// Start multi action
public class MultiStart extends RestBaseAction {
    private static final String DB_KEY = "db";
    private static final String LABEL_KEY = "label";

    private ExecuteEnv execEnv;

    public MultiStart(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction (ActionController controller) throws IllegalArgException {
        ExecuteEnv executeEnv = ExecuteEnv.getInstance();
        MultiStart action = new MultiStart(controller, executeEnv);
        controller.registerHandler(HttpMethod.POST, "/api/{db}/_multi_start", action);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        String db = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException("No database selected");
        }
        String label = request.getSingleParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected");
        }
        AuthorizationInfo authInfo = getAuthorizationInfo(request);
        String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, db);

        checkWritePriv(authInfo.fullUserName, fullDbName);

        if (redirectToMaster(request, response)) {
            return;
        }

        Map<String, String> properties = Maps.newHashMap();
        String[] keys = {LoadStmt.TIMEOUT_PROPERTY, LoadStmt.MAX_FILTER_RATIO_PROPERTY};
        for (String key : keys) {
            String value = request.getSingleParameter(key);
            if (!Strings.isNullOrEmpty(value)) {
                properties.put(key, value);
            }
        }
        execEnv.getMultiLoadMgr().startMulti(db, label, properties);
        sendResult(request, response, RestBaseResult.getOk());
    }
}

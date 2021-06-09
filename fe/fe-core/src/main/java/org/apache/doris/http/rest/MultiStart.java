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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

// Start multi action
public class MultiStart extends RestBaseAction {
    private ExecuteEnv execEnv;

    public MultiStart(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv executeEnv = ExecuteEnv.getInstance();
        MultiStart action = new MultiStart(controller, executeEnv);
        controller.registerHandler(HttpMethod.POST, "/api/{" + DB_KEY + "}/_multi_start", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        String db = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException("No database selected");
        }
        String label = request.getSingleParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected");
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // Multi start request must redirect to master, because all following sub requests will be handled
        // on Master
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
        for (String key : keys) {
            String value = request.getRequest().headers().get(key);
            if (!Strings.isNullOrEmpty(value)) {
                properties.put(key, value);
            }
        }
        execEnv.getMultiLoadMgr().startMulti(fullDbName, label, properties);
        sendResult(request, response, RestBaseResult.getOk());
    }
}


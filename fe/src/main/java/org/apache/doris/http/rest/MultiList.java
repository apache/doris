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
import com.google.common.collect.Lists;

import java.util.List;

import io.netty.handler.codec.http.HttpMethod;

// list all multi load before commit
public class MultiList extends RestBaseAction {

    private ExecuteEnv execEnv;

    public MultiList(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv executeEnv = ExecuteEnv.getInstance();
        MultiList action = new MultiList(controller, executeEnv);
        controller.registerHandler(HttpMethod.POST, "/api/{" + DB_KEY + "}/_multi_list", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        String db = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException("No database selected");
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        if (redirectToMaster(request, response)) {
            return;
        }

        final List<String> labels = Lists.newArrayList();
        execEnv.getMultiLoadMgr().list(fullDbName, labels);
        sendResult(request, response, new Result(labels));
    }

    private static class Result extends RestBaseResult {
        private List<String> labels;

        public Result(List<String> labels) {
            this.labels = labels;
        }
    }
}


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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import io.netty.handler.codec.http.HttpMethod;

// Get load information of one load job
public class GetLoadInfoAction extends RestBaseAction {
    public GetLoadInfoAction(ActionController controller, boolean isStreamLoad) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        GetLoadInfoAction action = new GetLoadInfoAction(controller, false);
        controller.registerHandler(HttpMethod.GET, "/api/{" + DB_KEY + "}/_load_info", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        Load.JobInfo info = new Load.JobInfo(request.getSingleParameter(DB_KEY),
                                             request.getSingleParameter(LABEL_KEY),
                                             ConnectContext.get().getClusterName());
        if (Strings.isNullOrEmpty(info.dbName)) {
            throw new DdlException("No database selected");
        }
        if (Strings.isNullOrEmpty(info.label)) {
            throw new DdlException("No label selected");
        }
        if (Strings.isNullOrEmpty(info.clusterName)) {
            throw new DdlException("No cluster name selected");
        }

        if (redirectToMaster(request, response)) {
            return;
        }

        try {
            catalog.getLoadInstance().getJobInfo(info);
            if (info.tblNames.isEmpty()) {
                checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), info.dbName, PrivPredicate.LOAD);
            } else {
                for (String tblName : info.tblNames) {
                    checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), info.dbName, tblName,
                            PrivPredicate.LOAD);
                }
            }
        } catch (DdlException | MetaNotFoundException e) {
            catalog.getLoadManager().getLoadJobInfo(info);
        }
        sendResult(request, response, new Result(info));
    }

    private static class Result extends RestBaseResult {
        private Load.JobInfo jobInfo;
        public Result(Load.JobInfo info) {
            jobInfo = info;
        }
    }
}

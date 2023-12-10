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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// Get load information of one load job
// To be compatible with old api, we return like this:
// {
//     "status": "OK",
//     "msg": "Success",
//     "jobInfo": {
//         "dbName": "default_cluster:db1",
//         "tblNames": ["tbl1"],
//         "label": "abc",
//         "clusterName": "default_cluster",
//         "state": "FINISHED",
//         "failMsg": "",
//         "trackingUrl": "\\N"
//     }
// }
@RestController
public class GetLoadInfoAction extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_load_info", method = RequestMethod.GET)
    public Object execute(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        String fullDbName = getFullDbName(dbName);

        Load.JobInfo info = new Load.JobInfo(fullDbName,
                request.getParameter(LABEL_KEY),
                ConnectContext.get().getClusterName());
        if (Strings.isNullOrEmpty(info.dbName)) {
            return new RestBaseResult("No database selected");
        }
        if (Strings.isNullOrEmpty(info.label)) {
            return new RestBaseResult("No label selected");
        }
        if (Strings.isNullOrEmpty(info.clusterName)) {
            return new RestBaseResult("No cluster selected");
        }

        Object redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        try {
            Env.getCurrentEnv().getLoadInstance().getJobInfo(info);
            if (info.tblNames.isEmpty()) {
                checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), info.dbName, PrivPredicate.LOAD);
            } else {
                for (String tblName : info.tblNames) {
                    checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), info.dbName, tblName,
                            PrivPredicate.LOAD);
                }
            }
        } catch (DdlException | MetaNotFoundException e) {
            try {
                Env.getCurrentEnv().getLoadManager().getLoadJobInfo(info);
            } catch (DdlException e1) {
                return new RestBaseResult(e.getMessage());
            }
        }
        return new Result(info);
    }

    // This is just same as Result in http/rest/GetLoadInfoAction.java
    // for compatibility.
    private static class Result extends RestBaseResult {
        public Load.JobInfo jobInfo;

        public Result(Load.JobInfo info) {
            jobInfo = info;
        }
    }
}

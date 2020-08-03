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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.http.entity.ResponseEntityBuilder;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.google.common.base.Strings;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// Get load information of one load job
@RestController
public class GetLoadInfoAction extends RestBaseController {

    protected Catalog catalog;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_load_info", method = RequestMethod.GET)
    public Object execute(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        this.catalog = Catalog.getCurrentCatalog();
        String fullDbName = getFullDbName(dbName);

        Load.JobInfo info = new Load.JobInfo(fullDbName,
                request.getParameter(LABEL_KEY),
                ConnectContext.get().getClusterName());
        if (Strings.isNullOrEmpty(info.dbName)) {
            return ResponseEntityBuilder.badRequest("No database selected");
        }
        if (Strings.isNullOrEmpty(info.label)) {
            return ResponseEntityBuilder.badRequest("No label selected");
        }
        if (Strings.isNullOrEmpty(info.clusterName)) {
            return ResponseEntityBuilder.badRequest("No cluster selected");
        }

        RedirectView redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
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
            try {
                catalog.getLoadManager().getLoadJobInfo(info);
            } catch (DdlException e1) {
                return ResponseEntityBuilder.okWithCommonError(e1.getMessage());
            }
        }
        return ResponseEntityBuilder.ok(info);
    }
}

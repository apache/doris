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
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.load.Load;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// Get load information of one load job
@RestController
public class GetLoadInfoAction extends RestBaseController {

    protected Catalog catalog;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_load_info",method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        this.catalog = Catalog.getCurrentCatalog();
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        Load.JobInfo info = new Load.JobInfo(request.getParameter(DB_KEY),
                request.getParameter(LABEL_KEY),
                ConnectContext.get().getClusterName());
        if (Strings.isNullOrEmpty(info.dbName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        if (Strings.isNullOrEmpty(info.label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }
        if (Strings.isNullOrEmpty(info.clusterName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No cluster selected");
            return entity;
        }

        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
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
        entity = ResponseEntity.status(HttpStatus.OK).build(new Result(info));
        return entity;
    }

    private static class Result extends RestBaseResult {
        private Load.JobInfo jobInfo;
        public Result(Load.JobInfo info) {
            jobInfo = info;
        }
    }
}

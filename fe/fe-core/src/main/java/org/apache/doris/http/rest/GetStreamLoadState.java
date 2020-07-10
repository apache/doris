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
import org.apache.doris.catalog.Database;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GetStreamLoadState extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/get_load_state", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity<String> entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        final String clusterName = ConnectContext.get().getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No cluster selected");
            return entity;
        }

        String dbName = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }

        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }

        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("unknown database, database=" + dbName);
            return entity;
        }

        String state = Catalog.getCurrentGlobalTransactionMgr().getLabelState(db.getId(), label).toString();
        entity.setData(state);
        return entity;
    }

    private static class Result extends RestBaseResult {
        private String state;
        public Result(String state) {
            this.state = state;
        }
    }
}

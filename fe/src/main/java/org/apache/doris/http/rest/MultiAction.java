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

import com.google.common.collect.Maps;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.view.RedirectView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// List all labels of one multi-load
public class MultiAction extends RestBaseController {
    private ExecuteEnv execEnv;
    private static final String SUB_LABEL_KEY = "sub_label";


    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_desc",method = RequestMethod.POST)
    public Object multi_desc(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }
        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        final List<String> labels = Lists.newArrayList();
        execEnv.getMultiLoadMgr().desc(fullDbName, label, labels);
        entity.setData(labels);
        return entity;
    }


    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_list",method = RequestMethod.POST)
    public Object multi_list(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        final List<String> labels = Lists.newArrayList();
        execEnv.getMultiLoadMgr().list(fullDbName, labels);
        entity.setData(labels);
        return entity;
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_start",method = RequestMethod.POST)
    public Object multi_start(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }
        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // Mutli start request must redirect to master, because all following sub requests will be handled
        // on Master
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        Map<String, String> properties = Maps.newHashMap();
        String[] keys = {LoadStmt.TIMEOUT_PROPERTY, LoadStmt.MAX_FILTER_RATIO_PROPERTY};
        for (String key : keys) {
            String value = request.getParameter(key);
            if (!Strings.isNullOrEmpty(value)) {
                properties.put(key, value);
            }
        }
        execEnv.getMultiLoadMgr().startMulti(fullDbName, label, properties);
        return entity;
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_unload",method = RequestMethod.POST)
    public Object multi_unload(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }

        String subLabel = request.getParameter(SUB_LABEL_KEY);
        if (Strings.isNullOrEmpty(subLabel)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No sub_label selected");
            return entity;
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        execEnv.getMultiLoadMgr().unload(fullDbName, label, subLabel);
        return entity;
    }


    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_commit",method = RequestMethod.POST)
    public Object multi_commit(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        execEnv.getMultiLoadMgr().commit(fullDbName, label);
        return entity;
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_abort",method = RequestMethod.POST)
    public Object multi_abort(HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        execEnv = ExecuteEnv.getInstance();
        String db = request.getParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No database selected");
            return entity;
        }
        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            entity.setCode(HttpStatus.NOT_FOUND.value());
            entity.setMsg("No label selected");
            return entity;
        }

        String fullDbName = ClusterNamespace.getFullName(ConnectContext.get().getClusterName(), db);
        checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        try {
            RedirectView redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        execEnv.getMultiLoadMgr().abort(fullDbName, label);
        return entity;
    }

    private static class Result extends RestBaseResult {
        private List<String> labels;

        public Result(List<String> labels) {
            this.labels = labels;
        }
    }
}


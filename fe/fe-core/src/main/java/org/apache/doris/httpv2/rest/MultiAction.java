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

import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.RestBaseResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


// In order to ensure compatibility with http v1, the return format of all our interfaces here remains the same as v1,
// that is, instead of using ResponseEntityBuilder to return, use the same format as v1 alone.
// v1 response:
// { "status" : "OK", "msg" : "some msg" }
@RestController
public class MultiAction extends RestBaseController {
    private static final String SUB_LABEL_KEY = "sub_label";

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_desc", method = RequestMethod.POST)
    public Object multi_desc(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String label = request.getParameter(LABEL_KEY);
            if (Strings.isNullOrEmpty(label)) {
                return new RestBaseResult("No label selected");
            }

            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            // only Master has these load info
            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }

            final List<String> labels = Lists.newArrayList();
            ExecuteEnv.getInstance().getMultiLoadMgr().desc(fullDbName, label, labels);
            return new MultiLabelResult(labels);
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_list", method = RequestMethod.POST)
    public Object multi_list(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            // only Master has these load info
            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }

            final List<String> labels = Lists.newArrayList();
            ExecuteEnv.getInstance().getMultiLoadMgr().list(fullDbName, labels);
            return new MultiLabelResult(labels);
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_start", method = RequestMethod.POST)
    public Object multi_start(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String label = request.getParameter(LABEL_KEY);
            if (Strings.isNullOrEmpty(label)) {
                return new RestBaseResult("No label selected");
            }
            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            // Multi start request must redirect to master, because all following sub requests will be handled
            // on Master

            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }

            Map<String, String> properties = Maps.newHashMap();
            String[] keys = {LoadStmt.TIMEOUT_PROPERTY, LoadStmt.MAX_FILTER_RATIO_PROPERTY, LoadStmt.KEY_COMMENT};
            for (String key : keys) {
                String value = request.getParameter(key);
                if (!Strings.isNullOrEmpty(value)) {
                    properties.put(key, value);
                }
            }
            for (String key : keys) {
                String value = request.getHeader(key);
                if (!Strings.isNullOrEmpty(value)) {
                    properties.put(key, value);
                }
            }
            ExecuteEnv.getInstance().getMultiLoadMgr().startMulti(fullDbName, label, properties);
            return RestBaseResult.getOk();
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_unload", method = RequestMethod.POST)
    public Object multi_unload(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String label = request.getParameter(LABEL_KEY);
            if (Strings.isNullOrEmpty(label)) {
                return new RestBaseResult("No label selected");
            }

            String subLabel = request.getParameter(SUB_LABEL_KEY);
            if (Strings.isNullOrEmpty(subLabel)) {
                return new RestBaseResult("No sub label selected");
            }

            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }

            ExecuteEnv.getInstance().getMultiLoadMgr().unload(fullDbName, label, subLabel);
            return RestBaseResult.getOk();
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_commit", method = RequestMethod.POST)
    public Object multi_commit(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String label = request.getParameter(LABEL_KEY);
            if (Strings.isNullOrEmpty(label)) {
                return new RestBaseResult("No label selected");
            }

            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            // only Master has these load info

            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }
            try {
                ExecuteEnv.getInstance().getMultiLoadMgr().commit(fullDbName, label);
            } catch (Exception e) {
                return new RestBaseResult(e.getMessage());
            }
            return RestBaseResult.getOk();
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_multi_abort", method = RequestMethod.POST)
    public Object multi_abort(
            @PathVariable(value = DB_KEY) final String dbName,
            HttpServletRequest request, HttpServletResponse response)
            throws DdlException {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        try {
            executeCheckPassword(request, response);

            String label = request.getParameter(LABEL_KEY);
            if (Strings.isNullOrEmpty(label)) {
                return new RestBaseResult("No label selected");
            }

            String fullDbName = getFullDbName(dbName);
            checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, PrivPredicate.LOAD);

            // only Master has these load info
            Object redirectView = redirectToMaster(request, response);
            if (redirectView != null) {
                return redirectView;
            }

            ExecuteEnv.getInstance().getMultiLoadMgr().abort(fullDbName, label);
            return RestBaseResult.getOk();
        } catch (Exception e) {
            return new RestBaseResult(e.getMessage());
        }
    }

    private static class MultiLabelResult extends RestBaseResult {
        private List<String> labels;

        public MultiLabelResult(List<String> labels) {
            this.labels = labels;
        }
    }
}

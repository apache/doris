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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GetStreamLoadState extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/get_load_state", method = RequestMethod.GET)
    public Object execute(@PathVariable(value = DB_KEY) final String dbName,
                          HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        Object redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            return ResponseEntityBuilder.badRequest("No label selected");
        }

        final String fullDbName = getFullDbName(dbName);

        Database db;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
            String state = Env.getCurrentGlobalTransactionMgr().getLabelState(db.getId(), label).toString();
            return ResponseEntityBuilder.ok(state);
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }
}

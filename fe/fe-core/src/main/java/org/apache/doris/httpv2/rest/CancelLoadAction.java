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
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * To cancel a load transaction with given load label
 */
@RestController
public class CancelLoadAction extends RestBaseController {

    @RequestMapping(path = "/api/{" + DB_KEY + "}/_cancel", method = RequestMethod.POST)
    public Object execute(@PathVariable(value = DB_KEY) final String dbName,
                          HttpServletRequest request, HttpServletResponse response) {
        if (needRedirect(request.getScheme())) {
            return redirectToHttps(request);
        }

        executeCheckPassword(request, response);
        Object redirectView = redirectToMaster(request, response);
        if (redirectView != null) {
            return redirectView;
        }

        if (Strings.isNullOrEmpty(dbName)) {
            return ResponseEntityBuilder.badRequest("No database selected");
        }

        String fullDbName = getFullDbName(dbName);

        String label = request.getParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            return ResponseEntityBuilder.badRequest("No label specified");
        }

        Database db;
        try {
            db = Env.getCurrentInternalCatalog().getDbOrMetaException(fullDbName);
        } catch (MetaNotFoundException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        // TODO(cmy): Currently we only check priv in db level.
        // Should check priv in table level.
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName,
                        PrivPredicate.LOAD)) {
            throw new UnauthorizedException("Access denied for user '" + ConnectContext.get().getQualifiedUser()
                    + "' to database '" + fullDbName + "'");
        }

        try {
            Env.getCurrentGlobalTransactionMgr().abortTransaction(db.getId(), label, "user cancel");
        } catch (UserException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        return ResponseEntityBuilder.ok();
    }

}

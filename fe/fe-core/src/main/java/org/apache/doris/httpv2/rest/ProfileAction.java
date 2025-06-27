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
import org.apache.doris.common.profile.ProfileManager;
import org.apache.doris.common.profile.ProfileManager.ProfileElement;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.exception.UnauthorizedException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This class is a RESTFUL interface to get query profile.
// It will be used in query monitor to collect profiles.
// Usage:
//      wget http://fe_host:fe_http_port/api/profile?query_id=123456
@RestController
public class ProfileAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(ProfileAction.class);

    @RequestMapping(path = "/api/profile", method = RequestMethod.GET)
    protected Object profile(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);

        String queryId = request.getParameter("query_id");
        if (Strings.isNullOrEmpty(queryId)) {
            return ResponseEntityBuilder.badRequest("Missing query_id");
        }
        ProfileElement profileElementObject = ProfileManager.getInstance().findProfileElementObject(queryId);
        if (profileElementObject == null) {
            return ResponseEntityBuilder.okWithCommonError("query id " + queryId + " not found.");
        }
        String user = profileElementObject.infoStrings.get(SummaryProfile.USER);
        if (!ConnectContext.get().getCurrentUserIdentity().getQualifiedUser().equals(user) || Env.getCurrentEnv()
                .getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new UnauthorizedException(
                    "Access denied; You can only view your own profile unless you have admin_priv.");
        }
        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr == null) {
            return ResponseEntityBuilder.okWithCommonError("query id " + queryId + " not found.");
        }

        Map<String, String> result = Maps.newHashMap();
        result.put("profile", queryProfileStr);
        return ResponseEntityBuilder.ok(result);
    }

    @RequestMapping(path = "/api/profile/text", method = RequestMethod.GET)
    protected Object profileText(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String queryId = request.getParameter("query_id");
        if (Strings.isNullOrEmpty(queryId)) {
            queryId = ProfileManager.getInstance().getLastProfileId();
        }

        String queryProfileStr = ProfileManager.getInstance().getProfile(queryId);
        if (queryProfileStr == null) {
            return "query id " + queryId + " not found";
        }

        return queryProfileStr;
    }
}

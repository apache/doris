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

import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This class is a RESTFUL interface to get query minidump.
// It will be used in query monitor to collect minidumps.
// Usage:
//      wget http://fe_host:fe_http_port/api/minidump?query_id=123456
@RestController
public class MinidumpAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(MinidumpAction.class);

    @RequestMapping(path = "/api/minidump", method = RequestMethod.GET)
    protected Object minidump(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        String queryId = request.getParameter("query_id");
        if (Strings.isNullOrEmpty(queryId)) {
            return ResponseEntityBuilder.badRequest("Missing query_id");
        }

        String queryMinidumpStr = MinidumpUtils.getMinidumpString(queryId);
        if (queryMinidumpStr == null) {
            return ResponseEntityBuilder.okWithCommonError("query id " + queryId + " not found.");
        }

        // Set the headers to indicate a file attachment and set the filename
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setContentDispositionFormData("attachment", queryId + ".json");

        // Return the file data as a byte array along with headers
        return ResponseEntity.ok()
            .headers(headers)
            .body(queryMinidumpStr);
    }
}

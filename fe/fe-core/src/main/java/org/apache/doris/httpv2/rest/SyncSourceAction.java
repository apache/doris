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

import org.apache.doris.common.util.AESUtil;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SyncSourceAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(SyncSourceAction.class);

    /**
     * Endpoint to initiate the synchronization of source data.
     *
     * <p>This method is triggered via a GET request to "/api/_sync_source".
     * It initializes the service's public key certificate by fetching it from a configured URL.
     *
     * @return ResponseEntity containing a success message or an error message with appropriate HTTP status.
     */
    @RequestMapping(path = "/api/_sync_source", method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        LOG.info("Received request to synchronize source secret key information.");
        try {
            AESUtil.syncSecretKeyFromUrl();
            return ResponseEntityBuilder.ok("Successfully synchronized source secret key information.");
        } catch (Exception e) {
            LOG.error("Failed to synchronize source secret key information.", e);
            return ResponseEntityBuilder.internalError(e.getMessage());
        }
    }
}

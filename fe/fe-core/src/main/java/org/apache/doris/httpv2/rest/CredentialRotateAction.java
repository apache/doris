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

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.httpv2.HttpTokenManager;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;

import com.google.common.base.Strings;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;

/**
 * Node-local credential rotation endpoints for FE.
 * Only accepts POST requests authenticated with this FE's query parameter `token`.
 * Never forwards to Master or broadcasts to other nodes.
 */
@RestController
public class CredentialRotateAction {
    private static final Logger LOG = LogManager.getLogger(CredentialRotateAction.class);
    private static final String TOKEN_PARAM = "token";

    private static final Object CLOUD_UNIQUE_ID_ROTATE_LOCK = new Object();

    @RequestMapping(path = "/api/rotate_http_token", method = RequestMethod.POST)
    public ResponseEntity rotateHttpToken(HttpServletRequest request) {
        String token = request.getParameter(TOKEN_PARAM);
        if (!HttpTokenManager.getInstance().verify(token)) {
            LOG.warn("Rejected rotate_http_token request: unauthorized");
            return ResponseEntityBuilder.unauthorized("incorrect token");
        }

        HttpTokenManager.RotateResult result = HttpTokenManager.getInstance().rotate();
        if (!result.isOk()) {
            return ResponseEntityBuilder.okWithCommonError(result.getErrorMsg());
        }
        return ResponseEntityBuilder.ok(Collections.emptyMap());
    }

    @RequestMapping(path = "/api/rotate_cloud_unique_id", method = RequestMethod.POST)
    public ResponseEntity rotateCloudUniqueId(HttpServletRequest request) {
        String token = request.getParameter(TOKEN_PARAM);
        if (!HttpTokenManager.getInstance().verify(token)) {
            LOG.warn("Rejected rotate_cloud_unique_id request: unauthorized");
            return ResponseEntityBuilder.unauthorized("incorrect token");
        }

        synchronized (CLOUD_UNIQUE_ID_ROTATE_LOCK) {
            String configuredId;
            try {
                configuredId = ConfigBase.readEffectiveConfigValue("cloud_unique_id");
            } catch (Exception e) {
                LOG.warn("Failed to read configured cloud_unique_id during rotation");
                return ResponseEntityBuilder.okWithCommonError("Failed to read configured cloud_unique_id");
            }
            if (Strings.isNullOrEmpty(configuredId)) {
                LOG.warn("Configured cloud_unique_id is empty during rotation");
                return ResponseEntityBuilder.okWithCommonError("Configured cloud_unique_id must not be empty");
            }

            if (!configuredId.equals(Config.cloud_unique_id)) {
                Config.cloud_unique_id = configuredId;
                LOG.info("rotate_cloud_unique_id succeeded");
            } else {
                LOG.info("rotate_cloud_unique_id succeeded (idempotent, unchanged)");
            }
            return ResponseEntityBuilder.ok(Collections.emptyMap());
        }
    }
}

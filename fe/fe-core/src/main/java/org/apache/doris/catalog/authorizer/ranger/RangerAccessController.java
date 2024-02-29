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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.CatalogAccessController;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

import java.util.Collection;

public abstract class RangerAccessController implements CatalogAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerAccessController.class);

    protected static final String CLIENT_TYPE_DORIS = "doris";

    protected static boolean checkRequestResult(RangerAccessRequestImpl request,
            RangerAccessResult result, String name) {
        if (result == null) {
            LOG.warn("Error getting authorizer result, please check your ranger config. Make sure "
                    + "ranger policy engine is initialized. Request: {}", request);
            return false;
        }

        if (result.getIsAllowed()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", request, result.getPolicyId());
            }
            return true;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        result.getAccessRequest().getResource().getAsString()));
            }
            return false;
        }
    }


    public static void checkRequestResults(Collection<RangerAccessResult> results, String name)
            throws AuthorizationException {
        for (RangerAccessResult result : results) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", result.getAccessRequest(), result.getPolicyId());
            }
            if (!result.getIsAllowed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(result.getReason());
                }
                throw new AuthorizationException(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        result.getAccessRequest().getResource().getAsString().replaceAll("/", ".")));
            }
        }
    }
}

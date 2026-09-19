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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.catalog.authorizer.ranger.RangerUserStoreGroups;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

public class RangerDorisPlugin extends RangerBasePlugin {
    private static final Logger LOG = LogManager.getLogger(RangerDorisPlugin.class);

    public RangerDorisPlugin(String serviceName) {
        this(serviceName, null);
    }

    public RangerDorisPlugin(String serviceName, RangerAuthContextListener rangerAuthContextListener) {
        super(serviceName, null, null);
        super.init();
        super.registerAuthContextEventListener(rangerAuthContextListener);
        LOG.info(RangerUserStoreGroups.describe(getConfig()));
    }

    /**
     * Takes the policies Ranger downloaded, and asks for the user store with them, so that the requests
     * {@code RangerDorisAccessController} builds can carry the groups Ranger keeps for a user; see
     * {@link RangerUserStoreGroups}. Done on the way in rather than left to Ranger because Ranger only does
     * it on its own from 2.5 on, and behind a property.
     */
    @Override
    public void setPolicies(ServicePolicies policies) {
        RangerUserStoreGroups.addUserStoreEnricher(getConfig(), policies);
        super.setPolicies(policies);
    }
}

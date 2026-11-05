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

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.util.ServicePolicies;

/** Mirrors the controller's private Boolean parent check; columns always use the original Ranger API. */
public final class DorisScopeBenchmarkAdapter implements V2AuthorizationBenchmark.Adapter {
    private final RangerDorisPlugin plugin;
    private ServicePolicies policies;
    private long nativeChecks;

    public DorisScopeBenchmarkAdapter(RangerPluginConfig configuration) {
        plugin = new RangerDorisPlugin(configuration);
    }

    @Override
    public void install(ServicePolicies policies) {
        this.policies = policies;
        plugin.setPolicies(policies);
    }

    @Override
    public RangerAccessResult evaluate(RangerAccessRequest request) {
        if ("SELECT".equals(request.getAccessType()) || "ADMIN".equals(request.getAccessType())) {
            RangerDorisResource resource = resource(request);
            if (resource != null && !plugin.mayGrantReadPrivilege(request.getUser(), resource)) {
                // The production controller returns Boolean false here. In this benchmark's static
                // column-only model, the omitted parents have no matching audit resource either.
                RangerAccessResult refused = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS,
                        policies.getServiceName(), policies.getServiceDef(), request);
                refused.setIsAllowed(false);
                refused.setIsAccessDetermined(true);
                refused.setIsAudited(false);
                return refused;
            }
        }
        nativeChecks++;
        return plugin.isAccessAllowed(request);
    }

    private RangerDorisResource resource(RangerAccessRequest request) {
        Object global = request.getResource().getValue("global");
        if (global != null) {
            return new RangerDorisResource(DorisObjectType.GLOBAL, global.toString());
        }
        Object catalog = request.getResource().getValue("catalog");
        Object database = request.getResource().getValue("database");
        Object table = request.getResource().getValue("table");
        if (catalog == null || request.getResource().getValue("column") != null) {
            return null;
        }
        if (table != null) {
            return new RangerDorisResource(DorisObjectType.TABLE,
                    catalog.toString(), database.toString(), table.toString());
        }
        if (database != null) {
            return new RangerDorisResource(DorisObjectType.DATABASE, catalog.toString(), database.toString());
        }
        return new RangerDorisResource(DorisObjectType.CATALOG, catalog.toString());
    }

    @Override
    public long version() {
        return plugin.getPoliciesVersion();
    }

    @Override
    public long nativeEngineCalls() {
        return nativeChecks;
    }

    @Override
    public String describe() {
        return "Doris compact policies + conservative parent-scope pruning";
    }

    @Override
    public void close() {
        policies = null;
        plugin.cleanup();
    }
}

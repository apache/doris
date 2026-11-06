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

/** Models the controller's Boolean boundary, including indexed column checks; not the raw Ranger API. */
public final class DorisIndexedReadBenchmarkAdapter implements V2AuthorizationBenchmark.Adapter {
    private final RangerDorisPlugin plugin;
    private ServicePolicies policies;
    private long nativeChecks;

    public DorisIndexedReadBenchmarkAdapter(RangerPluginConfig configuration) {
        configuration.set(configuration.getPropertyPrefix() + ".read.resource.index.enabled", "true");
        plugin = new RangerDorisPlugin(configuration);
    }

    @Override
    public void install(ServicePolicies policies) {
        this.policies = policies;
        plugin.setPolicies(policies);
    }

    @Override
    public RangerAccessResult evaluate(RangerAccessRequest request) {
        RangerDorisResource resource = resource(request);
        if (resource != null && ("SELECT".equals(request.getAccessType()) || "ADMIN".equals(request.getAccessType()))) {
            if (!plugin.mayGrantReadPrivilege(request.getUser(), resource)) {
                return decision(request, false);
            }
            Boolean indexed = plugin.indexedReadPrivilege(request.getUser(),
                    DorisAccessType.valueOf(request.getAccessType()), resource);
            if (indexed != null) {
                return decision(request, indexed);
            }
        }
        nativeChecks++;
        return plugin.isAccessAllowed(request);
    }

    private RangerAccessResult decision(RangerAccessRequest request, boolean allowed) {
        RangerAccessResult result = new RangerAccessResult(RangerPolicy.POLICY_TYPE_ACCESS,
                policies.getServiceName(), policies.getServiceDef(), request);
        result.setIsAllowed(allowed);
        result.setIsAccessDetermined(true);
        return result;
    }

    private static RangerDorisResource resource(RangerAccessRequest request) {
        Object global = request.getResource().getValue("global");
        if (global != null) {
            return new RangerDorisResource(DorisObjectType.GLOBAL, global.toString());
        }
        Object catalog = request.getResource().getValue("catalog");
        Object database = request.getResource().getValue("database");
        Object table = request.getResource().getValue("table");
        Object column = request.getResource().getValue("column");
        if (catalog == null) {
            return null;
        }
        if (column != null) {
            return new RangerDorisResource(DorisObjectType.COLUMN,
                    catalog.toString(), database.toString(), table.toString(), column.toString());
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
        return "Doris Boolean controller with native matcher references indexed by table and user";
    }

    @Override
    public void close() {
        policies = null;
        plugin.cleanup();
    }
}

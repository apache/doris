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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisResource;

import com.google.common.collect.Lists;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collection;
import java.util.List;

/**
 * A Ranger policy engine whose answers are a pure function of (user, resource, access type), so that a
 * behaviour baseline recorded against it stays byte-identical across runs and machines.
 *
 * <p>Only the policy engine is faked. The controller above it is the production
 * {@link org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController}, so its own
 * global-then-catalog-then-database-then-table cascade, its SHOW-as-subtree probe and its privilege-bit
 * decomposition are all exercised for real.
 *
 * <p>The whole policy set, and nothing else, is:
 * <ul>
 *   <li>{@code ranger_user} may SELECT {@code <any catalog>.pdb.ptbl} and its column {@code pcol1};</li>
 *   <li>{@code ranger_user} may USAGE the resource {@code res1}, workload group {@code wg1},
 *       storage vault {@code sv1} and compute group {@code cg1};</li>
 *   <li>a subtree probe (the request Ranger sends for SHOW: no access type, SELF_OR_DESCENDANTS) succeeds for
 *       {@code ranger_user} on any ancestor of {@code pdb.ptbl};</li>
 *   <li>{@code ranger_user} reading {@code pdb.ptbl} gets the row filter {@code pcol1 = 1} and a MASK_NULL
 *       mask on {@code pcol1};</li>
 *   <li>every other user - including the ones holding built-in ADMIN_PRIV and NODE_PRIV - is denied
 *       everywhere, and gets no filter and no mask.</li>
 * </ul>
 *
 * <p>Denying the built-in admin is the point: wherever the baseline records ALLOW for that user on a
 * Ranger-governed resource, the decision provably did not come from Ranger. It comes from the controller
 * deferring to whoever owns global scope, and pinning it here is what keeps that deference from being dropped
 * or widened unnoticed.
 */
public class StubRangerPolicyEngine extends RangerBasePlugin {
    public static final String ALLOWED_USER = "ranger_user";
    public static final String ALLOWED_DB = "pdb";
    public static final String ALLOWED_TABLE = "ptbl";
    public static final String ALLOWED_COLUMN = "pcol1";
    public static final String ALLOWED_RESOURCE = "res1";
    public static final String ALLOWED_WORKLOAD_GROUP = "wg1";
    public static final String ALLOWED_STORAGE_VAULT = "sv1";
    public static final String ALLOWED_COMPUTE_GROUP = "cg1";

    public static final String ROW_FILTER_EXPR = "pcol1 = 1";
    public static final long ROW_FILTER_POLICY_ID = 101L;
    public static final long ROW_FILTER_POLICY_VERSION = 7L;
    public static final long DATA_MASK_POLICY_ID = 202L;
    public static final long DATA_MASK_POLICY_VERSION = 9L;

    private static final String SELECT_ACCESS_TYPE = "SELECT";
    private static final String USAGE_ACCESS_TYPE = "USAGE";

    public StubRangerPolicyEngine() {
        super("stub-ranger-doris", null, null);
    }

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
        RangerAccessResult result = new RangerAccessResult(1, "stub", null, request);
        result.setPolicyVersion(1L);
        result.setIsAllowed(isAllowed(request));
        return result;
    }

    @Override
    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
        List<RangerAccessResult> results = Lists.newArrayList();
        for (RangerAccessRequest request : requests) {
            results.add(isAccessAllowed(request));
        }
        return results;
    }

    @Override
    public RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request,
            RangerAccessResultProcessor resultProcessor) {
        RangerAccessResult result = new RangerAccessResult(3, "stub", null, request);
        result.setPolicyId(ROW_FILTER_POLICY_ID);
        result.setPolicyVersion(ROW_FILTER_POLICY_VERSION);
        RangerAccessResource resource = request.getResource();
        if (ALLOWED_USER.equals(request.getUser())
                && ALLOWED_DB.equals(value(resource, RangerDorisResource.KEY_DATABASE))
                && ALLOWED_TABLE.equals(value(resource, RangerDorisResource.KEY_TABLE))) {
            result.setFilterExpr(ROW_FILTER_EXPR);
        }
        return result;
    }

    @Override
    public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
            RangerAccessResultProcessor resultProcessor) {
        RangerAccessResult result = new RangerAccessResult(2, "stub", null, request);
        result.setPolicyId(DATA_MASK_POLICY_ID);
        result.setPolicyVersion(DATA_MASK_POLICY_VERSION);
        RangerAccessResource resource = request.getResource();
        if (ALLOWED_USER.equals(request.getUser())
                && ALLOWED_DB.equals(value(resource, RangerDorisResource.KEY_DATABASE))
                && ALLOWED_TABLE.equals(value(resource, RangerDorisResource.KEY_TABLE))
                && ALLOWED_COLUMN.equals(value(resource, RangerDorisResource.KEY_COLUMN))) {
            result.setMaskType("MASK_NULL");
        }
        return result;
    }

    private boolean isAllowed(RangerAccessRequest request) {
        if (!ALLOWED_USER.equals(request.getUser())) {
            return false;
        }
        RangerAccessResource resource = request.getResource();
        String db = value(resource, RangerDorisResource.KEY_DATABASE);
        String tbl = value(resource, RangerDorisResource.KEY_TABLE);
        String col = value(resource, RangerDorisResource.KEY_COLUMN);
        String accessType = request.getAccessType();

        if (ResourceMatchingScope.SELF_OR_DESCENDANTS == request.getResourceMatchingScope()) {
            // The subtree probe Ranger sends for SHOW: "does this user hold anything at or below here?".
            // It carries no access type of its own, so it must be recognised by its matching scope.
            return isPrefixOfAllowedTable(resource, db, tbl);
        }
        if (SELECT_ACCESS_TYPE.equals(accessType)) {
            return ALLOWED_DB.equals(db) && ALLOWED_TABLE.equals(tbl)
                    && (col == null || ALLOWED_COLUMN.equals(col));
        }
        if (USAGE_ACCESS_TYPE.equals(accessType)) {
            return ALLOWED_RESOURCE.equals(value(resource, RangerDorisResource.KEY_RESOURCE))
                    || ALLOWED_WORKLOAD_GROUP.equals(value(resource, RangerDorisResource.KEY_WORKLOAD_GROUP))
                    || ALLOWED_STORAGE_VAULT.equals(value(resource, RangerDorisResource.KEY_STORAGE_VAULT))
                    || ALLOWED_COMPUTE_GROUP.equals(value(resource, RangerDorisResource.KEY_COMPUTE_GROUP));
        }
        return false;
    }

    private boolean isPrefixOfAllowedTable(RangerAccessResource resource, String db, String tbl) {
        if (value(resource, RangerDorisResource.KEY_CATALOG) == null) {
            // A global / resource / workload group probe is never an ancestor of the one allowed table.
            return false;
        }
        return (db == null || ALLOWED_DB.equals(db))
                && (tbl == null || ALLOWED_TABLE.equals(tbl))
                && value(resource, RangerDorisResource.KEY_COLUMN) == null;
    }

    private static String value(RangerAccessResource resource, String key) {
        Object value = resource.getValue(key);
        return value == null ? null : value.toString();
    }
}

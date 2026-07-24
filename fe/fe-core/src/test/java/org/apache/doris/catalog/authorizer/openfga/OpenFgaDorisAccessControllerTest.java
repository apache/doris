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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OpenFgaDorisAccessControllerTest {

    /**
     * A fake checker that records every request and allows only the tuples it was told to grant.
     * It does not resolve the OpenFGA model itself; the tests assert the exact (user, relation,
     * object) tuples the controller emits and the allow/deny it derives from them.
     */
    private static class RecordingChecker implements OpenFgaChecker {
        private final Set<OpenFgaCheckRequest> granted = new HashSet<>();
        private final List<OpenFgaCheckRequest> seen = new ArrayList<>();

        void grant(String user, String relation, String object) {
            granted.add(new OpenFgaCheckRequest(user, relation, object));
        }

        @Override
        public boolean check(OpenFgaCheckRequest request) {
            seen.add(request);
            return granted.contains(request);
        }

        boolean sawRequest(String user, String relation, String object) {
            return seen.contains(new OpenFgaCheckRequest(user, relation, object));
        }
    }

    private RecordingChecker checker;
    private OpenFgaDorisAccessController controller;
    private UserIdentity analyst;

    @Before
    public void setUp() {
        checker = new RecordingChecker();
        Map<String, String> props = new HashMap<>();
        props.put(OpenFgaConfig.API_URL, "http://localhost:8080");
        props.put(OpenFgaConfig.STORE_ID, "01TESTSTORE");
        OpenFgaConfig config = OpenFgaConfig.fromProps(props);
        controller = new OpenFgaDorisAccessController(checker, config);
        analyst = UserIdentity.createAnalyzedUserIdentWithIp("analyst", "%");
    }

    @Test
    public void testTableSelectMapsToCanSelectOnTableObject() {
        checker.grant("user:analyst", "can_select", "table:internal/db1/tbl1");

        boolean result = controller.checkTblPriv(analyst, "internal", "db1", "tbl1", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_select", "table:internal/db1/tbl1"));
    }

    @Test
    public void testTableSelectDeniedWhenNothingGranted() {
        boolean result = controller.checkTblPriv(analyst, "internal", "db1", "tbl1", PrivPredicate.SELECT);

        Assert.assertFalse(result);
        // The controller still probed the table level with the right tuple before denying.
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_select", "table:internal/db1/tbl1"));
    }

    @Test
    public void testGlobalAdminGrantsTableSelect() {
        checker.grant("user:analyst", "can_admin", "global:doris");

        boolean result = controller.checkTblPriv(analyst, "internal", "db1", "tbl1", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_admin", "global:doris"));
    }

    @Test
    public void testCatalogSelectMapsToCatalogObject() {
        checker.grant("user:analyst", "can_select", "catalog:hive_ctl");

        boolean result = controller.checkCtlPriv(analyst, "hive_ctl", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_select", "catalog:hive_ctl"));
    }

    @Test
    public void testDbLoadMapsToDatabaseObject() {
        checker.grant("user:analyst", "can_load", "database:internal/db1");

        boolean result = controller.checkDbPriv(analyst, "internal", "db1", PrivPredicate.LOAD);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_load", "database:internal/db1"));
    }

    @Test
    public void testColsPrivPassesWhenColumnSelectGranted() throws AuthorizationException {
        checker.grant("user:analyst", "can_select", "column:internal/db1/tbl1/c1");

        controller.checkColsPriv(analyst, "internal", "db1", "tbl1", ImmutableSet.of("c1"), PrivPredicate.SELECT);

        Assert.assertTrue(checker.sawRequest("user:analyst", "can_select", "column:internal/db1/tbl1/c1"));
    }

    @Test
    public void testColsPrivThrowsWhenColumnDenied() {
        Assert.assertThrows(AuthorizationException.class, () ->
                controller.checkColsPriv(analyst, "internal", "db1", "tbl1",
                        ImmutableSet.of("c1"), PrivPredicate.SELECT));
    }

    @Test
    public void testResourceUsageMapsToResourceObject() {
        checker.grant("user:analyst", "can_usage", "resource:s3_repo");

        boolean result = controller.checkResourcePriv(analyst, "s3_repo", PrivPredicate.USAGE);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_usage", "resource:s3_repo"));
    }

    @Test
    public void testStorageVaultUsageMapsToStorageVaultObject() {
        checker.grant("user:analyst", "can_usage", "storage_vault:vault1");

        boolean result = controller.checkStorageVaultPriv(analyst, "vault1", PrivPredicate.USAGE);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_usage", "storage_vault:vault1"));
    }

    @Test
    public void testWorkloadGroupDefaultGroupAlwaysAllowed() {
        boolean result = controller.checkWorkloadGroupPriv(analyst, WorkloadGroupMgr.DEFAULT_GROUP_NAME,
                PrivPredicate.USAGE);

        Assert.assertTrue(result);
        // The default group is short circuited, so no OpenFGA call is made.
        Assert.assertFalse(checker.sawRequest("user:analyst", "can_usage", "workload_group:normal"));
    }

    @Test
    public void testWorkloadGroupNonDefaultMapsToWorkloadGroupObject() {
        checker.grant("user:analyst", "can_usage", "workload_group:etl");

        boolean result = controller.checkWorkloadGroupPriv(analyst, "etl", PrivPredicate.USAGE);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_usage", "workload_group:etl"));
    }

    @Test
    public void testCloudClusterUsageMapsToComputeGroupObject() {
        checker.grant("user:analyst", "can_usage", "compute_group:cg1");

        boolean result = controller.checkCloudPriv(analyst, "cg1", PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER);

        Assert.assertTrue(result);
        Assert.assertTrue(checker.sawRequest("user:analyst", "can_usage", "compute_group:cg1"));
    }

    @Test
    public void testCloudNonClusterTypeIsDenied() {
        boolean result = controller.checkCloudPriv(analyst, "cg1", PrivPredicate.USAGE, ResourceTypeEnum.GENERAL);

        Assert.assertFalse(result);
    }

    @Test
    public void testFailClosedCheckerCausesDeny() {
        // The fail closed behavior itself lives in OpenFgaClientWrapper (see its test): on an OpenFGA
        // error it returns false. Here we confirm the controller honors that: a checker that denies
        // every tuple (as a failing checker would) results in an overall deny, never an allow.
        RecordingChecker denyAll = new RecordingChecker();
        Map<String, String> props = new HashMap<>();
        props.put(OpenFgaConfig.API_URL, "http://localhost:8080");
        props.put(OpenFgaConfig.STORE_ID, "01TESTSTORE");
        OpenFgaDorisAccessController denyController =
                new OpenFgaDorisAccessController(denyAll, OpenFgaConfig.fromProps(props));

        Assert.assertFalse(denyController.checkTblPriv(analyst, "internal", "db1", "tbl1", PrivPredicate.SELECT));
    }

    @Test
    public void testEvalPoliciesAreEmptyInMvp() {
        Assert.assertFalse(controller.evalDataMaskPolicy(analyst, "internal", "db1", "tbl1", "c1").isPresent());
        Assert.assertTrue(controller.evalRowFilterPolicies(analyst, "internal", "db1", "tbl1").isEmpty());
    }
}

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

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.catalog.authorizer.ranger.doris.DorisAccessType;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisResource;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class RangerTest {

    public static class DorisTestPlugin extends RangerBasePlugin {
        public DorisTestPlugin(String serviceName) {
            super(serviceName, null, null);
            // super.init();
        }

        @Override
        public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
            List<RangerAccessResult> results = Lists.newArrayList();
            for (RangerAccessRequest request : requests) {
                RangerAccessResult result = isAccessAllowed(request);
                if (result != null) {
                    results.add(result);
                }
            }
            return results;
        }

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            RangerAccessResource resource = request.getResource();
            String ctl = (String) resource.getValue(RangerDorisResource.KEY_CATALOG);
            String db = (String) resource.getValue(RangerDorisResource.KEY_DATABASE);
            String tbl = (String) resource.getValue(RangerDorisResource.KEY_TABLE);
            String col = (String) resource.getValue(RangerDorisResource.KEY_COLUMN);
            String rs = (String) resource.getValue(RangerDorisResource.KEY_RESOURCE);
            String wg = (String) resource.getValue(RangerDorisResource.KEY_WORKLOAD_GROUP);
            String cg = (String) resource.getValue(RangerDorisResource.KEY_COMPUTE_GROUP);
            String sv = (String) resource.getValue(RangerDorisResource.KEY_STORAGE_VAULT);
            String user = request.getUser();
            return returnAccessResult(request, ctl, db, tbl, col, rs, wg, cg, sv, user);
        }

        @Override
        public RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                RangerAccessResultProcessor resultProcessor) {
            RangerAccessResource resource = request.getResource();
            String ctl = (String) resource.getValue(RangerDorisResource.KEY_CATALOG);
            String db = (String) resource.getValue(RangerDorisResource.KEY_DATABASE);
            String tbl = (String) resource.getValue(RangerDorisResource.KEY_TABLE);
            String col = (String) resource.getValue(RangerDorisResource.KEY_COLUMN);

            RangerAccessResult result = new RangerAccessResult(2, "test", null, request);
            result.setPolicyVersion(1L);
            if ("ctl1".equals(ctl) && "db1".equals(db) && "tbl1".equals(tbl) && "col1".equals(col)) {
                result.addAdditionalInfo("maskType", "MASK_NULL");
            } else if ("ctl1".equals(ctl) && "db1".equals(db) && "tbl1".equals(tbl) && "col2".equals(col)) {
                result.addAdditionalInfo("maskType", "MASK_NONE");
            } else if ("ctl1".equals(ctl) && "db1".equals(db) && "tbl1".equals(tbl) && "col3".equals(col)) {
                result.addAdditionalInfo("maskType", "CUSTOM");
                result.addAdditionalInfo("maskedValue", "hex({col})");
            } else {
                // Unable to mock other mask type
                result.addAdditionalInfo("maskType", "");
            }
            return result;
        }

        private RangerAccessResult returnAccessResult(
                RangerAccessRequest request, String ctl, String db, String tbl,
                String col, String rs, String wg, String cg, String sv, String user) {
            RangerAccessResult result = new RangerAccessResult(1, "test", null, request);
            if (!Strings.isNullOrEmpty(wg)) {
                result.setIsAllowed(wg.equals("wg1"));
            } else if (!Strings.isNullOrEmpty(rs)) {
                result.setIsAllowed(wg.equals("rs1"));
            } else if (!Strings.isNullOrEmpty(col)) {
                boolean res = ("ctl1".equals(ctl) && "db1".equals(db) && "tbl1".equals(tbl) && "col1".equals(col))
                        || ("ctl1".equals(ctl) && "db1".equals(db) && "tbl1".equals(tbl) && "col2".equals(col));
                result.setIsAllowed(res);
            } else if (!Strings.isNullOrEmpty(tbl)) {
                result.setIsAllowed("ctl2".equals(ctl) && "db2".equals(db) && "tbl2".equals(tbl));
            } else if (!Strings.isNullOrEmpty(db)) {
                result.setIsAllowed("ctl3".equals(ctl) && "db3".equals(db));
            } else if (!Strings.isNullOrEmpty(ctl)) {
                result.setIsAllowed("ctl4".equals(ctl));
            } else if (!Strings.isNullOrEmpty(cg)) {
                result.setIsAllowed("cg1".equals(cg));
            } else if (!Strings.isNullOrEmpty(sv)) {
                result.setIsAllowed("sv1".equals(sv));
            } else {
                result.setIsAllowed(false);
            }
            return result;
        }
    }

    /**
     * Grants one action per level of the hierarchy and counts what was asked, so that an action a level
     * already granted being asked about again further down is observable.
     */
    public static class LevelledPlugin extends RangerBasePlugin {
        private final AtomicInteger requests = new AtomicInteger();

        public LevelledPlugin() {
            super("test", null, null);
        }

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            requests.incrementAndGet();
            RangerAccessResource resource = request.getResource();
            RangerAccessResult result = new RangerAccessResult(1, "test", null, request);
            if (resource.getValue(RangerDorisResource.KEY_GLOBAL) != null) {
                result.setIsAllowed(false);
            } else if (resource.getValue(RangerDorisResource.KEY_TABLE) == null) {
                result.setIsAllowed(DorisAccessType.SELECT.name().equals(request.getAccessType()));
            } else {
                result.setIsAllowed(DorisAccessType.LOAD.name().equals(request.getAccessType()));
            }
            return result;
        }
    }

    private static final AuthorizedSubject USER = AuthorizedSubject.of("user1", "%");
    /** Resource and workload group usage, as the engine asks about it. */
    private static final AccessRequirement USAGE = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.USAGE, AccessAction.CLUSTER_USAGE, AccessAction.STAGE_USAGE);

    /**
     * An FE where nothing outside Ranger grants anything, so every verdict below is Ranger's own.
     */
    private static final AuthorizationContext NOTHING_GRANTED_ELSEWHERE = new AuthorizationContext() {
        @Override
        public Set<String> rolesOf(AuthorizedSubject subject) {
            return Collections.emptySet();
        }

        @Override
        public boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement) {
            return false;
        }
    };

    private RangerDorisAccessController controller() {
        return new RangerDorisAccessController(new DorisTestPlugin("test"), NOTHING_GRANTED_ELSEWHERE);
    }

    private void check(AuthorizedResource resource, AccessRequirement requirement) throws AccessDeniedException {
        controller().checkPrivilege(USER, resource, requirement, AccessContext.NONE);
    }

    private void assertRefused(AuthorizedResource resource, AccessRequirement requirement) {
        Assertions.assertThrows(AccessDeniedException.class, () -> check(resource, requirement));
    }

    // Does not have priv on ctl1.db1.tbl1.col3
    @Test
    public void testNoAuthCol() {
        assertRefused(AuthorizedResource.columns("ctl1", "db1", "tbl1", Sets.newHashSet("col1", "col3")),
                AccessRequirements.SELECT);
    }

    // Have priv on ctl1.db1.tbl1.col1 & col2
    @Test
    public void testAuthCol() throws AccessDeniedException {
        check(AuthorizedResource.columns("ctl1", "db1", "tbl1", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
    }

    // Have priv on ctl2.db2.tbl2, so when checking auth on col1 & col2, can pass
    @Test
    public void testUsingTableAuthAsColAuth() throws AccessDeniedException {
        check(AuthorizedResource.columns("ctl2", "db2", "tbl2", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
    }

    // Does not have priv on ctl2.db2.tbl3, so when checking auth on col1 & col2, can not pass
    @Test
    public void testUsingNoTableAuthAsColAuth() {
        assertRefused(AuthorizedResource.columns("ctl2", "db2", "tbl3", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
    }

    // Have priv on ctl3.db3, so when checking auth on tbl1 and (tbl1.col1 & tbl1.col2), can pass
    @Test
    public void testUsingDbAuthAsColAndTableAuth() throws AccessDeniedException {
        check(AuthorizedResource.columns("ctl3", "db3", "tbl1", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
        check(AuthorizedResource.table("ctl3", "db3", "tbl1"), AccessRequirements.SELECT);
    }


    // Does not have priv on ctl2.db3, so when checking auth on col1 & col2, can not pass
    @Test
    public void testNoDbAuthAsColAndTableAuth() {
        assertRefused(AuthorizedResource.columns("ctl2", "db3", "tbl3", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
    }

    // Have priv on ctl4, so when checking auth on objs under ctl4, can pass
    @Test
    public void testUsingCtlAuthAsColAndTableAndDbAuth() throws AccessDeniedException {
        check(AuthorizedResource.columns("ctl4", "db1", "tbl1", Sets.newHashSet("col1", "col2")),
                AccessRequirements.SELECT);
        check(AuthorizedResource.table("ctl4", "db2", "tbl2"), AccessRequirements.SELECT);
        check(AuthorizedResource.database("ctl4", "db3"), AccessRequirements.SELECT);
    }

    /**
     * An action granted at an outer level is not asked about again further in.
     *
     * <p>Doris checks a whole requirement, and this source answers it one action per request while walking
     * the resource down to the table. Carrying what is already granted is what keeps that walk at one request
     * per action rather than one per action per level - a difference in cost, not in verdict, so nothing that
     * records only allow/deny can hold it still.
     */
    @Test
    public void testAnActionGrantedAtAnOuterLevelIsNotAskedAboutAgain() throws AccessDeniedException {
        LevelledPlugin plugin = new LevelledPlugin();
        RangerDorisAccessController controller =
                new RangerDorisAccessController(plugin, NOTHING_GRANTED_ELSEWHERE);

        controller.checkPrivilege(USER, AuthorizedResource.table("ctl", "db", "tbl"),
                AccessRequirement.allOf(AccessAction.SELECT, AccessAction.LOAD), AccessContext.NONE);

        // Both actions at global (2) and at catalog (2, where SELECT is granted), then only the outstanding
        // LOAD at database (1) and at table (1), where it is granted.
        Assertions.assertEquals(6, plugin.requests.get());
    }

    /** The refusal names the column that failed, which is the whole reason columns answer with a message. */
    @Test
    public void testRefusalNamesTheColumnThatFailed() {
        AccessDeniedException refused = Assertions.assertThrows(AccessDeniedException.class,
                () -> check(AuthorizedResource.columns("ctl1", "db1", "tbl1", Sets.newHashSet("col3")),
                        AccessRequirements.SELECT));
        Assertions.assertEquals("Permission denied: user ['user1'@'%'] does not have privilege for"
                + " [ANY[ADMIN, SELECT]] command on [ctl1].[db1].[tbl1].[col3]", refused.getMessage());
    }

    @Test
    public void testDataMask() {
        Map<String, DataMaskSpec> masks = controller().getDataMasks(USER,
                AuthorizedResource.table("ctl1", "db1", "tbl1"),
                Sets.newHashSet("col1", "col2", "col3", "col4"), AccessContext.NONE);
        // MASK_NULL
        Assertions.assertEquals("NULL", masks.get("col1").getMaskSql());
        // MASK_NONE
        Assertions.assertFalse(masks.containsKey("col2"));
        // CUSTOM
        Assertions.assertEquals("hex(col3)", masks.get("col3").getMaskSql());
        // Others
        Assertions.assertFalse(masks.containsKey("col4"));
    }

    @Test
    public void testComputeGroupAuth() throws AccessDeniedException {
        check(AuthorizedResource.cloud(ResourceKind.CLOUD_COMPUTE_GROUP, "cg1"), USAGE);
        assertRefused(AuthorizedResource.cloud(ResourceKind.CLOUD_COMPUTE_GROUP, "cg2"), USAGE);
    }

    @Test
    public void testStorageVaultAuth() throws AccessDeniedException {
        check(AuthorizedResource.storageVault("sv1"), USAGE);
        assertRefused(AuthorizedResource.storageVault("sv2"), USAGE);
    }
}

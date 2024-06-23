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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisResource;
import org.apache.doris.common.AuthorizationException;

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
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
            String user = request.getUser();
            return returnAccessResult(request, ctl, db, tbl, col, rs, wg, user);
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
                String col, String rs, String wg, String user) {
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
            } else {
                result.setIsAllowed(false);
            }
            return result;
        }
    }

    // Does not have priv on ctl1.db1.tbl1.col3
    @Test(expected = AuthorizationException.class)
    public void testNoAuthCol() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col3");
        ac.checkColsPriv(ui, "ctl1", "db1", "tbl1", cols, PrivPredicate.SELECT);
    }

    // Have priv on ctl1.db1.tbl1.col1 & col2
    @Test
    public void testAuthCol() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl1", "db1", "tbl1", cols, PrivPredicate.SELECT);
    }

    // Have priv on ctl2.db2.tbl2, so when checking auth on col1 & col2, can pass
    @Test
    public void testUsingTableAuthAsColAuth() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl2", "db2", "tbl2", cols, PrivPredicate.SELECT);
    }

    // Does not have priv on ctl2.db2.tbl3, so when checking auth on col1 & col2, can not pass
    @Test(expected = AuthorizationException.class)
    public void testUsingNoTableAuthAsColAuth() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl2", "db2", "tbl3", cols, PrivPredicate.SELECT);
    }

    // Have priv on ctl3.db3, so when checking auth on tbl1 and (tbl1.col1 & tbl1.col2), can pass
    @Test
    public void testUsingDbAuthAsColAndTableAuth() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl3", "db3", "tbl1", cols, PrivPredicate.SELECT);
        ac.checkTblPriv(ui, "ctl3", "db3", "tbl1", PrivPredicate.SELECT);
    }


    // Does not have priv on ctl2.db3, so when checking auth on col1 & col2, can not pass
    @Test(expected = AuthorizationException.class)
    public void testNoDbAuthAsColAndTableAuth() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl2", "db3", "tbl3", cols, PrivPredicate.SELECT);
    }

    // Have priv on ctl4, so when checking auth on objs under ctl4, can pass
    @Test
    public void testUsingCtlAuthAsColAndTableAndDbAuth() throws AuthorizationException {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        Set<String> cols = Sets.newHashSet();
        cols.add("col1");
        cols.add("col2");
        ac.checkColsPriv(ui, "ctl4", "db1", "tbl1", cols, PrivPredicate.SELECT);
        ac.checkTblPriv(ui, "ctl4", "db2", "tbl2", PrivPredicate.SELECT);
        ac.checkDbPriv(ui, "ctl4", "db3", PrivPredicate.SELECT);
    }

    @Test
    public void testDataMask() {
        DorisTestPlugin plugin = new DorisTestPlugin("test");
        RangerDorisAccessController ac = new RangerDorisAccessController(plugin);
        UserIdentity ui = UserIdentity.createAnalyzedUserIdentWithIp("user1", "%");
        // MASK_NULL
        Optional<DataMaskPolicy> policy = ac.evalDataMaskPolicy(ui, "ctl1", "db1", "tbl1", "col1");
        Assertions.assertEquals("NULL", policy.get().getMaskTypeDef());
        // MASK_NONE
        policy = ac.evalDataMaskPolicy(ui, "ctl1", "db1", "tbl1", "col2");
        Assertions.assertTrue(!policy.isPresent());
        // CUSTOM
        policy = ac.evalDataMaskPolicy(ui, "ctl1", "db1", "tbl1", "col3");
        Assertions.assertEquals("hex(col3)", policy.get().getMaskTypeDef());
        // Others
        policy = ac.evalDataMaskPolicy(ui, "ctl1", "db1", "tbl1", "col4");
        Assertions.assertTrue(!policy.isPresent());
    }
}

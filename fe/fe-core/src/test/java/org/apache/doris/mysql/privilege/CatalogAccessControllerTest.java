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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AuthorizationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class CatalogAccessControllerTest {

    private static class StubAccessController implements CatalogAccessController {
        final AtomicBoolean ctlPrivCalled = new AtomicBoolean(false);
        final AtomicBoolean dbPrivCalled = new AtomicBoolean(false);
        final AtomicBoolean tblPrivCalled = new AtomicBoolean(false);
        final AtomicBoolean colsPrivCalled = new AtomicBoolean(false);

        private final boolean ctlResult;
        private final boolean dbResult;
        private final boolean tblResult;
        private final boolean colsResult;

        StubAccessController() {
            this(false, false, false, false);
        }

        StubAccessController(boolean ctlResult, boolean dbResult, boolean tblResult) {
            this(ctlResult, dbResult, tblResult, false);
        }

        StubAccessController(boolean ctlResult, boolean dbResult, boolean tblResult, boolean colsResult) {
            this.ctlResult = ctlResult;
            this.dbResult = dbResult;
            this.tblResult = tblResult;
            this.colsResult = colsResult;
        }

        @Override
        public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
            return false;
        }

        @Override
        public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
            ctlPrivCalled.set(true);
            return ctlResult;
        }

        @Override
        public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
            dbPrivCalled.set(true);
            return dbResult;
        }

        @Override
        public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
            tblPrivCalled.set(true);
            return tblResult;
        }

        @Override
        public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl,
                Set<String> cols, PrivPredicate wanted) throws AuthorizationException {
            colsPrivCalled.set(true);
            if (!colsResult) {
                throw new AuthorizationException("denied");
            }
        }

        @Override
        public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
            return false;
        }

        @Override
        public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName,
                PrivPredicate wanted) {
            return false;
        }

        @Override
        public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
                PrivPredicate wanted, ResourceTypeEnum type) {
            return false;
        }

        @Override
        public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName,
                PrivPredicate wanted) {
            return false;
        }

        @Override
        public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db,
                String tbl, String col) {
            return Optional.empty();
        }

        @Override
        public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl,
                String db, String tbl) {
            return ImmutableList.of();
        }
    }

    @Test
    public void testCheckCtlPrivShortCircuitOnHasGlobal() {
        StubAccessController controller = new StubAccessController();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkCtlPriv(true, user, "ctl", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertFalse(controller.ctlPrivCalled.get());
    }

    @Test
    public void testCheckCtlPrivFallsThroughWithoutHasGlobal() {
        StubAccessController controller = new StubAccessController(true, false, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkCtlPriv(false, user, "ctl", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(controller.ctlPrivCalled.get());
    }

    @Test
    public void testCheckCtlPrivFallsThroughAndReturnsFalse() {
        StubAccessController controller = new StubAccessController(false, false, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkCtlPriv(false, user, "ctl", PrivPredicate.SELECT);

        Assert.assertFalse(result);
        Assert.assertTrue(controller.ctlPrivCalled.get());
    }

    @Test
    public void testCheckDbPrivShortCircuitOnHasGlobal() {
        StubAccessController controller = new StubAccessController();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkDbPriv(true, user, "ctl", "db", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertFalse(controller.dbPrivCalled.get());
    }

    @Test
    public void testCheckDbPrivFallsThroughWithoutHasGlobal() {
        StubAccessController controller = new StubAccessController(false, true, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkDbPriv(false, user, "ctl", "db", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(controller.dbPrivCalled.get());
    }

    @Test
    public void testCheckDbPrivFallsThroughAndReturnsFalse() {
        StubAccessController controller = new StubAccessController(false, false, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkDbPriv(false, user, "ctl", "db", PrivPredicate.SELECT);

        Assert.assertFalse(result);
        Assert.assertTrue(controller.dbPrivCalled.get());
    }

    @Test
    public void testCheckTblPrivShortCircuitOnHasGlobal() {
        StubAccessController controller = new StubAccessController();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkTblPriv(true, user, "ctl", "db", "tbl", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertFalse(controller.tblPrivCalled.get());
    }

    @Test
    public void testCheckTblPrivFallsThroughWithoutHasGlobal() {
        StubAccessController controller = new StubAccessController(false, false, true);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkTblPriv(false, user, "ctl", "db", "tbl", PrivPredicate.SELECT);

        Assert.assertTrue(result);
        Assert.assertTrue(controller.tblPrivCalled.get());
    }

    @Test
    public void testCheckTblPrivFallsThroughAndReturnsFalse() {
        StubAccessController controller = new StubAccessController(false, false, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        boolean result = controller.checkTblPriv(false, user, "ctl", "db", "tbl", PrivPredicate.SELECT);

        Assert.assertFalse(result);
        Assert.assertTrue(controller.tblPrivCalled.get());
    }

    @Test
    public void testCheckColsPrivShortCircuitOnHasGlobal() throws AuthorizationException {
        StubAccessController controller = new StubAccessController();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        controller.checkColsPriv(true, user, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);

        Assert.assertFalse(controller.colsPrivCalled.get());
    }

    @Test
    public void testCheckColsPrivFallsThroughWithoutHasGlobal() throws AuthorizationException {
        StubAccessController controller = new StubAccessController(false, false, false, true);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        controller.checkColsPriv(false, user, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT);

        Assert.assertTrue(controller.colsPrivCalled.get());
    }

    @Test
    public void testCheckColsPrivFallsThroughAndThrows() {
        StubAccessController controller = new StubAccessController(false, false, false, false);
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%");

        Assert.assertThrows(AuthorizationException.class, () ->
                controller.checkColsPriv(false, user, "ctl", "db", "tbl", ImmutableSet.of("col1"), PrivPredicate.SELECT));
        Assert.assertTrue(controller.colsPrivCalled.get());
    }
}

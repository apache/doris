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
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.common.AuthorizationException;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A source written against the deprecated {@link CatalogAccessController}, so that a catalog bound to one is
 * a column of the behaviour baseline.
 *
 * <p>The point of it is the adapter. Implementations of that interface live outside this repository and are
 * promised they keep working, and the route they take through the engine -
 * {@link LegacyAccessControllerPlugin} - is the only route whose behaviour a rework can change without
 * changing anything a plugin of the current contract does. A baseline whose external catalogs are all bound
 * to sources of the current contract cannot see that.
 *
 * <p>Everything it decides is a constant: {@link #GRANTEE} may read {@link #ALLOWED_DB} and only that,
 * {@link #ALLOWED_COLUMN} and only that, subject to one restrictive row filter and one column mask; nobody
 * else may do anything, and it grants nothing at all at global scope - which is what makes the exemption the
 * adapter reproduces visible in the matrix rather than hidden behind the source's own answer.
 */
public class StubLegacyAccessControllerFactory implements AccessControllerFactory {

    /** The name this stub is selected by, i.e. what a catalog writes in {@code access_controller.class}. */
    public static final String NAME = "stub-legacy";

    /** The one account this source grants anything to; the same probe user the built-in half grants. */
    public static final String GRANTEE = "local_user";
    /** The db, table and column are the ones the whole matrix probes, so the column is a real column. */
    public static final String ALLOWED_DB = StubRangerPolicyEngine.ALLOWED_DB;
    public static final String ALLOWED_TABLE = StubRangerPolicyEngine.ALLOWED_TABLE;
    public static final String ALLOWED_COLUMN = StubRangerPolicyEngine.ALLOWED_COLUMN;

    @Override
    public String factoryIdentifier() {
        return NAME;
    }

    @Override
    public CatalogAccessController createAccessController(Map<String, String> prop) {
        return new StubLegacyAccessController();
    }

    /** The controller itself; see the factory's documentation for what it decides. */
    public static class StubLegacyAccessController implements CatalogAccessController {

        private static boolean isGrantee(UserIdentity currentUser) {
            return currentUser != null && GRANTEE.equals(currentUser.getUser());
        }

        @Override
        public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
            return false;
        }

        @Override
        public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
            return isGrantee(currentUser);
        }

        @Override
        public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
            return isGrantee(currentUser) && ALLOWED_DB.equals(db);
        }

        @Override
        public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl,
                PrivPredicate wanted) {
            return isGrantee(currentUser) && ALLOWED_DB.equals(db) && ALLOWED_TABLE.equals(tbl);
        }

        @Override
        public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl,
                Set<String> cols, PrivPredicate wanted) throws AuthorizationException {
            if (!checkTblPriv(currentUser, ctl, db, tbl, wanted)) {
                throw new AuthorizationException("no privilege on " + db + "." + tbl);
            }
            for (String col : cols) {
                if (!ALLOWED_COLUMN.equalsIgnoreCase(col)) {
                    throw new AuthorizationException("no privilege on column [" + col + "]");
                }
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
        public boolean checkCloudPriv(UserIdentity currentUser, String cloudName, PrivPredicate wanted,
                ResourceTypeEnum type) {
            return false;
        }

        @Override
        public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName,
                PrivPredicate wanted) {
            return false;
        }

        @Override
        public Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db,
                String tbl, String col) {
            if (!isGrantee(currentUser) || !ALLOWED_COLUMN.equalsIgnoreCase(col)) {
                return Optional.empty();
            }
            return Optional.of(new DataMaskSpec("stub-legacy-mask:1", "NULL"));
        }

        @Override
        public List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
                String tbl) {
            if (!isGrantee(currentUser) || !ALLOWED_TABLE.equals(tbl)) {
                return ImmutableList.of();
            }
            return ImmutableList.of(RowFilterSpec.restrictive("stub-legacy-filter:1",
                    ALLOWED_COLUMN + " = 1"));
        }
    }
}

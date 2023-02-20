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
import org.apache.doris.common.AuthorizationException;

import java.util.Set;

public interface CatalogAccessController {
    // ==== Catalog ====
    default boolean checkCtlPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        boolean res = checkCtlPriv(currentUser, ctl, wanted);
        return hasGlobal || res;
    }

    boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted);

    // ==== Database ====
    default boolean checkDbPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, String db,
            PrivPredicate wanted) {
        boolean res = checkDbPriv(currentUser, ctl, db, wanted);
        return hasGlobal || res;
    }

    boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted);

    // ==== Table ====
    default boolean checkTblPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, String db, String tbl,
            PrivPredicate wanted) {
        boolean res = checkTblPriv(currentUser, ctl, db, tbl, wanted);
        return hasGlobal || res;
    }

    boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted);

    // ==== Column ====
    default void checkColsPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, String db, String tbl,
            Set<String> cols, PrivPredicate wanted) throws AuthorizationException {
        try {
            checkColsPriv(currentUser, ctl, db, tbl, cols, wanted);
        } catch (AuthorizationException e) {
            if (!hasGlobal) {
                throw e;
            }
        }
    }

    void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl,
            Set<String> cols, PrivPredicate wanted) throws AuthorizationException;
}

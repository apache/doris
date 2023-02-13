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

public class InternalCatalogAccessController implements CatalogAccessController {
    private Auth auth;

    public InternalCatalogAccessController(Auth auth) {
        this.auth = auth;
    }

    // ==== Catalog ====
    @Override
    public boolean checkCtlPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return hasGlobal || auth.checkCtlPriv(currentUser, ctl, wanted);
    }

    // ==== Database ====
    @Override
    public boolean checkDbPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, String db,
            PrivPredicate wanted) {
        return hasGlobal || auth.checkDbPriv(currentUser, ctl, db, wanted);
    }

    // ==== Table ====
    @Override
    public boolean checkTblPriv(boolean hasGlobal, UserIdentity currentUser, String ctl, String db, String tbl,
            PrivPredicate wanted) {
        return hasGlobal || auth.checkTblPriv(currentUser, ctl, db, tbl, wanted);
    }
}

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

package org.apache.doris.catalog.authorizer.ranger.cache;

import org.apache.doris.analysis.UserIdentity;

import com.google.common.base.Objects;

public class RowFilterCacheKey {
    private UserIdentity userIdentity;
    private String ctl;
    private String db;
    private String tbl;

    public RowFilterCacheKey(UserIdentity userIdentity, String ctl, String db, String tbl) {
        this.userIdentity = userIdentity;
        this.ctl = ctl;
        this.db = db;
        this.tbl = tbl;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public String getCtl() {
        return ctl;
    }

    public String getDb() {
        return db;
    }

    public String getTbl() {
        return tbl;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowFilterCacheKey that = (RowFilterCacheKey) o;
        return Objects.equal(userIdentity, that.userIdentity)
                && Objects.equal(ctl, that.ctl) && Objects.equal(db, that.db)
                && Objects.equal(tbl, that.tbl);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(userIdentity, ctl, db, tbl);
    }

    @Override
    public String toString() {
        return "DatamaskCacheKey{"
                + "userIdentity=" + userIdentity
                + ", ctl='" + ctl + '\''
                + ", db='" + db + '\''
                + ", tbl='" + tbl + '\''
                + '}';
    }
}

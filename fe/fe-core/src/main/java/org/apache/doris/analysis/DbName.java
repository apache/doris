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

package org.apache.doris.analysis;

import org.apache.doris.datasource.InternalCatalog;

import java.util.Objects;

public class DbName {
    private String ctl;
    private String db;

    public DbName(String ctl, String db) {
        this.ctl = ctl;
        this.db = db;
    }

    public String getCtl() {
        return ctl;
    }

    public void setCtl(String ctl) {
        this.ctl = ctl;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (ctl != null && !ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            stringBuilder.append(ctl).append(".");
        }
        stringBuilder.append(db);
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof DbName) {
            return toString().equals(other.toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctl, db);
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (ctl != null && !ctl.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            stringBuilder.append("`").append(ctl).append("`.");
        }
        stringBuilder.append("`").append(db).append("`");
        return stringBuilder.toString();
    }
}

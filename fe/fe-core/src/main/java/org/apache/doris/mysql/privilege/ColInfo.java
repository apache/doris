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

import java.util.Objects;

public class ColInfo {
    private String ctl;
    private String db;
    private String table;
    private String col;

    public ColInfo(String ctl, String db, String table, String col) {
        this.ctl = ctl;
        this.db = db;
        this.table = table;
        this.col = col;
    }

    public String getCol() {
        return col;
    }

    public String getCtl() {
        return ctl;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColInfo colInfo = (ColInfo) o;
        return Objects.equals(ctl, colInfo.ctl) && Objects.equals(db, colInfo.db)
                && Objects.equals(table, colInfo.table) && Objects.equals(col, colInfo.col);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctl, db, table, col);
    }
}

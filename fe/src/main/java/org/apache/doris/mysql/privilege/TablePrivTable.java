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

import org.apache.doris.common.io.Text;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.io.DataOutput;
import java.io.IOException;

/*
 * TablePrivTable saves all table level privs
 */
public class TablePrivTable extends PrivTable {

    /*
     * Return all privs which match the user@host on db.tbl
     * All returned privs will be saved in 'savedPrivs'.
     * If the given db or table is null, it will not check if database or table is match 
     */
    public void getPrivs(String host, String db, String user, String tbl, PrivBitSet savedPrivs) {
        TablePrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;

            // check host
            if (!tblPrivEntry.isAnyHost() && !tblPrivEntry.getHostPattern().match(host)) {
                continue;
            }

            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (db != null && !tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check user
            if (!tblPrivEntry.isAnyUser() && !tblPrivEntry.getUserPattern().match(user)) {
                continue;
            }

            // check table
            if (tbl != null && !tblPrivEntry.getTblPattern().match(tbl)) {
                continue;
            }

            matchedEntry = tblPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    public boolean hasPrivsOfDb(String host, String db, String user) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;

            // check host
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check user
            if (!tblPrivEntry.isAnyUser() && !tblPrivEntry.getUserPattern().match(user)) {
                continue;
            }

            return true;
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = TablePrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }

    public boolean hasClusterPriv(ConnectContext ctx, String clusterName) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            if (tblPrivEntry.getOrigDb().startsWith(clusterName)) {
                return true;
            }
        }
        return false;
    }
}

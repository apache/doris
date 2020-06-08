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
     * Return first priv which match the user@host on db.tbl The returned priv will
     * be saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String db, String tbl, PrivBitSet savedPrivs) {
        TablePrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            if (!tblPrivEntry.match(currentUser, true)) {
                continue;
            }

            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check table
            if (!tblPrivEntry.getTblPattern().match(tbl)) {
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

    /*
     * Check if user@host has specified privilege on any table
     */
    public boolean hasPriv(String host, String user, PrivPredicate wanted) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            // check host
            if (!tblPrivEntry.isAnyHost() && !tblPrivEntry.getHostPattern().match(host)) {
                continue;
            }
            // check user
            if (!tblPrivEntry.isAnyUser() && !tblPrivEntry.getUserPattern().match(user)) {
                continue;
            }
            // check priv
            if (tblPrivEntry.privSet.satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasPrivsOfDb(UserIdentity currentUser, String db) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;

            if (!tblPrivEntry.match(currentUser, true)) {
                continue;
            }

            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
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

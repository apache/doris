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

import org.apache.doris.mysql.privilege.PrivObject.PrivObjectType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/*
 * TablePrivTable saves all table level privs
 */
public class TablePrivTable extends PrivTable {

    /*
     * Return first priv which match the user@host on ctl.db.tbl The returned priv will
     * be saved in 'savedPrivs'.
     */
    public void getPrivs(String ctl, String db, String tbl, PrivBitSet savedPrivs) {
        TablePrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            // check catalog
            if (!tblPrivEntry.isAnyCtl() && !tblPrivEntry.getCtlPattern().match(ctl)) {
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

    public boolean hasPrivsOfCatalog(String ctl) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            // check catalog
            Preconditions.checkState(!tblPrivEntry.isAnyCtl());
            if (tblPrivEntry.getCtlPattern().match(ctl)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasPrivsOfDb(String ctl, String db) {
        for (PrivEntry entry : entries) {
            TablePrivEntry
                    tblPrivEntry = (TablePrivEntry) entry;

            // check catalog
            Preconditions.checkState(!tblPrivEntry.isAnyCtl());
            if (!tblPrivEntry.getCtlPattern().match(ctl)) {
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

    public boolean hasClusterPriv(String clusterName) {
        for (PrivEntry entry : entries) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            if (tblPrivEntry.getOrigDb().startsWith(clusterName)) {
                return true;
            }
        }
        return false;
    }

    public List<PrivObject> getPrivObjects() {
        List<PrivObject> res = Lists.newArrayList();
        for (PrivEntry entry : entries) {
            TablePrivEntry tablePrivEntry = (TablePrivEntry) entry;
            String origCtl = tablePrivEntry.getOrigCtl();
            String origDb = tablePrivEntry.getOrigDb();
            String origTbl = tablePrivEntry.getOrigTbl();
            PrivBitSet privSet = tablePrivEntry.getPrivSet();
            if (!privSet.isEmpty()) {
                res.add(new PrivObject(origCtl, origDb, origTbl, null, PrivObjectType.TABLE,
                        privSet.toPrivilegeNames()));
            }
        }
        return res;
    }
}

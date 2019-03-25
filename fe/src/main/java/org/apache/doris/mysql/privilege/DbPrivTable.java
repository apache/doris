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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/*
 * DbPrivTable saves all database level privs
 */
public class DbPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(DbPrivTable.class);

    public void getPrivs(String host, String db, String user, PrivBitSet savedPrivs) {
        DbPrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;

            // check host
            if (!dbPrivEntry.isAnyHost() && !dbPrivEntry.getHostPattern().match(host)) {
                continue;
            }

            // check db
            if (!dbPrivEntry.isAnyDb() && !dbPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check user
            if (!dbPrivEntry.isAnyUser() && !dbPrivEntry.getUserPattern().match(user)) {
                continue;
            }

            matchedEntry = dbPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    public boolean hasClusterPriv(ConnectContext ctx, String clusterName) {
        for (PrivEntry entry : entries) {
            DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;
            if (dbPrivEntry.getOrigDb().startsWith(clusterName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = DbPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}

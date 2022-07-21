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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/*
 * CatalogPrivTable saves all catalog level privs
 */
public class CatalogPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(CatalogPrivTable.class);

    /*
     * Return first priv which match the user@host on ctl.* The returned priv will be
     * saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String ctl, PrivBitSet savedPrivs) {
        CatalogPrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            CatalogPrivEntry dsPrivEntry = (CatalogPrivEntry) entry;

            if (!dsPrivEntry.match(currentUser, true)) {
                continue;
            }

            // check catalog
            if (!dsPrivEntry.isAnyCtl() && !dsPrivEntry.getCtlPattern().match(ctl)) {
                continue;
            }

            matchedEntry = dsPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = CatalogPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}

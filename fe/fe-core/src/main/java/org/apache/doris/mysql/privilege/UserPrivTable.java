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
import org.apache.doris.datasource.InternalCatalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/*
 * UserPrivTable saves all global privs and also password for users
 */
@Deprecated
public class UserPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(UserPrivTable.class);

    public UserPrivTable() {
    }

    /**
     * When replay UserPrivTable from journal whose FeMetaVersion < VERSION_111, the global-level privileges should
     * degrade to internal-catalog-level privileges.
     */
    public CatalogPrivTable degradeToInternalCatalogPriv() throws IOException {
        CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
        for (PrivEntry privEntry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) privEntry;
            if (!globalPrivEntry.match(UserIdentity.ROOT, true)
                    && !globalPrivEntry.match(UserIdentity.ADMIN, true)
                    && !globalPrivEntry.privSet.isEmpty()) {
                try {
                    // USAGE_PRIV, NODE_PRIV and ADMIN_PRIV are no need to degrade.
                    PrivBitSet privsAfterRemoved = globalPrivEntry.privSet.copy();
                    privsAfterRemoved.unset(Privilege.USAGE_PRIV.getIdx());
                    privsAfterRemoved.unset(Privilege.NODE_PRIV.getIdx());
                    privsAfterRemoved.unset(Privilege.ADMIN_PRIV.getIdx());
                    CatalogPrivEntry entry = CatalogPrivEntry.create(globalPrivEntry.origUser, globalPrivEntry.origHost,
                            InternalCatalog.INTERNAL_CATALOG_NAME, globalPrivEntry.isDomain, privsAfterRemoved);
                    entry.setSetByDomainResolver(false);
                    catalogPrivTable.addEntry(entry, false, false);
                    // only keep USAGE_PRIV, NODE_PRIV and ADMIN_PRIV in global entry, if they exist before.
                    globalPrivEntry.privSet.and(
                            PrivBitSet.of(Privilege.USAGE_PRIV, Privilege.NODE_PRIV, Privilege.ADMIN_PRIV));
                } catch (Exception e) {
                    throw new IOException(e.getMessage());
                }
            }
        }
        return catalogPrivTable;
    }
}

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/*
 * GlobalPrivTable saves all global privs and also password for users
 */
public class GlobalPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(GlobalPrivTable.class);

    public GlobalPrivTable() {
    }

    public void getPrivs(PrivBitSet savedPrivs) {
        GlobalPrivEntry matchedEntry = null;
        for (PrivEntry entry : entries) {
            GlobalPrivEntry globalPrivEntry = (GlobalPrivEntry) entry;

            matchedEntry = globalPrivEntry;
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
            String className = GlobalPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }

    /**
     * When replay GlobalPrivTable from journal whose FeMetaVersion < VERSION_111, the global-level privileges should
     * degrade to internal-catalog-level privileges.
     */
    public CatalogPrivTable degradeToInternalCatalogPriv() throws IOException {
        CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
        // TODO: 2023/1/17 implement
        return catalogPrivTable;
    }
}

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

package org.apache.doris.datasource.paimon;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.fs.Path;

/** Upgrade checks for SDK features that cannot be preserved by Paimon 2.1. */
public final class PaimonCatalogCompatibility {
    private PaimonCatalogCompatibility() {
    }

    /**
     * Validate a newly created catalog inside its authentication context. On rejection,
     * close the catalog before transferring ownership to the caller.
     */
    public static Catalog requireSupportedPrivileges(Catalog catalog) throws Exception {
        try {
            Catalog root = DelegateCatalog.rootCatalog(catalog);
            // This is the same catalog scope and pair of markers used by Paimon 1.4's
            // PrivilegedCatalog/FileBasedPrivilegeManager. REST catalogs have no such ACL.
            if (root instanceof AbstractCatalog) {
                AbstractCatalog fileCatalog = (AbstractCatalog) root;
                String warehouse = fileCatalog.warehouse();
                if (fileCatalog.fileIO().exists(new Path(warehouse, "user.sys"))
                        && fileCatalog.fileIO().exists(new Path(warehouse, "privilege.sys"))) {
                    throw new UnsupportedOperationException("Paimon 2.1 no longer supports legacy warehouse "
                            + "privileges: " + warehouse + ". Migrate authorization before upgrading; "
                            + "the legacy user.sys and privilege.sys metadata cannot be ignored.");
                }
            }
            return catalog;
        } catch (Exception failure) {
            try {
                catalog.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }
}

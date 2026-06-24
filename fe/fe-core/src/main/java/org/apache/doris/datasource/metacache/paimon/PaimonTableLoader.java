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

package org.apache.doris.datasource.metacache.paimon;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;

import org.apache.paimon.table.Table;

import java.io.IOException;

/**
 * Loads the base Paimon table handle used by cache entries and runtime projections.
 */
public final class PaimonTableLoader {

    public Table load(NameMapping nameMapping) {
        try {
            return catalog(nameMapping).getPaimonTable(nameMapping);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon table %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }

    public PaimonExternalCatalog catalog(NameMapping nameMapping) throws IOException {
        return (PaimonExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrException(nameMapping.getCtlId(), id -> new IOException("Catalog not found: " + id));
    }
}

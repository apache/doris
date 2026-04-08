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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import org.apache.paimon.table.Table;

import java.util.Optional;

public class PaimonUtils {

    public static Table getPaimonTable(ExternalTable dorisTable) {
        return paimonExternalMetaCache(dorisTable).getPaimonTable(dorisTable);
    }

    public static PaimonSnapshotCacheValue getLatestSnapshotCacheValue(ExternalTable dorisTable) {
        return paimonExternalMetaCache(dorisTable).getSnapshotCache(dorisTable);
    }

    public static PaimonSnapshotCacheValue getSnapshotCacheValue(Optional<MvccSnapshot> snapshot,
            ExternalTable dorisTable) {
        if (snapshot.isPresent() && snapshot.get() instanceof PaimonMvccSnapshot) {
            return ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        }
        return getLatestSnapshotCacheValue(dorisTable);
    }

    public static PaimonSchemaCacheValue getSchemaCacheValue(ExternalTable dorisTable,
            PaimonSnapshotCacheValue snapshotValue) {
        return getSchemaCacheValue(dorisTable, snapshotValue.getSnapshot().getSchemaId());
    }

    public static PaimonSchemaCacheValue getSchemaCacheValue(ExternalTable dorisTable, long schemaId) {
        return paimonExternalMetaCache(dorisTable)
                .getPaimonSchemaCacheValue(dorisTable.getOrBuildNameMapping(), schemaId);
    }

    private static PaimonExternalMetaCache paimonExternalMetaCache(ExternalTable table) {
        return Env.getCurrentEnv().getExtMetaCacheMgr().paimon(table.getCatalog().getId());
    }
}

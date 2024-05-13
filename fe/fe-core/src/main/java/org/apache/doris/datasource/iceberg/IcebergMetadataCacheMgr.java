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

package org.apache.doris.datasource.iceberg;

import java.util.concurrent.ExecutorService;

public class IcebergMetadataCacheMgr {

    private IcebergMetadataCache icebergMetadataCache;

    public IcebergMetadataCacheMgr(ExecutorService executor) {
        this.icebergMetadataCache = new IcebergMetadataCache(executor);
    }

    public IcebergMetadataCache getIcebergMetadataCache() {
        return icebergMetadataCache;
    }

    public void removeCache(long catalogId) {
        icebergMetadataCache.invalidateCatalogCache(catalogId);
    }

    public void invalidateCatalogCache(long catalogId) {
        icebergMetadataCache.invalidateCatalogCache(catalogId);
    }

    public void invalidateTableCache(long catalogId, String dbName, String tblName) {
        icebergMetadataCache.invalidateTableCache(catalogId, dbName, tblName);
    }

    public void invalidateDbCache(long catalogId, String dbName) {
        icebergMetadataCache.invalidateDbCache(catalogId, dbName);
    }
}

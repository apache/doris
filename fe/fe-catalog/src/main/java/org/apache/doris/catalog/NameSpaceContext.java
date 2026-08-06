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

package org.apache.doris.catalog;

/**
 * NameSpaceContext provides catalog/database context for name resolution.
 * Used to decouple name analysis from ConnectContext.
 */
public class NameSpaceContext {
    public static final String INTERNAL_CATALOG_NAME = "internal";

    private final String defaultCatalog;
    private final String currentDb;
    private final long currentDbId;

    public NameSpaceContext(String defaultCatalog, String currentDb, long currentDbId) {
        this.defaultCatalog = defaultCatalog;
        this.currentDb = currentDb;
        this.currentDbId = currentDbId;
    }

    public String getDefaultCatalog() {
        return defaultCatalog;
    }

    public String getCurrentDb() {
        return currentDb;
    }

    public long getCurrentDbId() {
        return currentDbId;
    }
}

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

package org.apache.doris.common.cache;

import org.apache.doris.nereids.SqlCacheContext.FullTableName;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SqlCacheMetadataVersionsTest {
    private final SqlCacheMetadataVersions versions = new SqlCacheMetadataVersions();
    private final FullTableName table = new FullTableName("catalog", "db", "table");

    private SqlCacheMetadataVersions.Snapshot snapshot(long id, FullTableName name) {
        SqlCacheMetadataVersions.Snapshot snapshot = versions.snapshot();
        snapshot.addTable(id, name);
        return snapshot;
    }

    @ParameterizedTest
    @CsvSource({"id", "name", "database", "catalog", "all"})
    void affectedDependenciesChange(String scope) {
        SqlCacheMetadataVersions.Snapshot affected = snapshot(1L, table);
        SqlCacheMetadataVersions.Snapshot other = snapshot(2L, new FullTableName("other", "db", "table"));
        switch (scope) {
            case "id":
                versions.invalidateTable(1L, null);
                break;
            case "name":
                versions.invalidateTable(-1L, table);
                break;
            case "database":
                versions.invalidateNamespace("catalog", "db");
                break;
            case "catalog":
                versions.invalidateNamespace("catalog", null);
                break;
            case "all":
                versions.invalidateAll();
                break;
            default:
                throw new AssertionError(scope);
        }
        Assertions.assertFalse(affected.isCurrent());
        Assertions.assertEquals(!scope.equals("all"), other.isCurrent());
        Assertions.assertTrue(snapshot(1L, table).isCurrent());
    }

    @Test
    void tableChangePreservesOtherTablesInTheSameNamespace() {
        SqlCacheMetadataVersions.Snapshot target = snapshot(1L, table);
        SqlCacheMetadataVersions.Snapshot other = snapshot(2L, new FullTableName("catalog", "db", "other"));
        versions.invalidateTable(1L, table);
        Assertions.assertFalse(target.isCurrent());
        Assertions.assertTrue(other.isCurrent());
    }

    @Test
    void dependencyRecordedAfterRefreshCannotAdoptANewRevision() {
        SqlCacheMetadataVersions.Snapshot oldQuery = versions.snapshot();
        versions.invalidateNamespace("catalog", "db");
        oldQuery.addTable(1L, table);
        Assertions.assertFalse(oldQuery.isCurrent());
        Assertions.assertTrue(snapshot(1L, table).isCurrent());
    }

    @Test
    void recordingTheSameTableAgainCannotReviveAnOldDependency() {
        SqlCacheMetadataVersions.Snapshot oldQuery = snapshot(1L, table);
        versions.invalidateTable(1L, table);
        oldQuery.addTable(1L, table);
        Assertions.assertFalse(oldQuery.isCurrent());
    }

    @Test
    void globalInvalidationIncludesQueriesWithoutTables() {
        SqlCacheMetadataVersions.Snapshot constant = versions.snapshot();
        versions.invalidateAll();
        Assertions.assertFalse(constant.isCurrent());
        Assertions.assertTrue(versions.snapshot().isCurrent());
    }
}

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

package org.apache.doris.paimon;

import com.google.common.base.Preconditions;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

final class PaimonTableCache {
    private static final ConcurrentHashMap<String, TableCacheEntry> TABLE_CACHE = new ConcurrentHashMap<>();

    private PaimonTableCache() {
    }

    static TableCacheEntry acquire(String cacheKey) {
        return TABLE_CACHE.computeIfPresent(cacheKey, (key, entry) -> {
            entry.users++;
            return entry;
        });
    }

    static boolean publish(String cacheKey, TableCacheEntry entry) {
        return TABLE_CACHE.putIfAbsent(cacheKey, entry) == null;
    }

    static void release(String cacheKey, TableCacheEntry expectedEntry) {
        TABLE_CACHE.compute(cacheKey, (key, currentEntry) -> {
            Preconditions.checkState(currentEntry == expectedEntry,
                    "Paimon table cache entry changed unexpectedly for key %s", cacheKey);
            Preconditions.checkState(currentEntry.users > 0,
                    "Paimon table cache reference count is invalid for key %s", cacheKey);
            currentEntry.users--;
            return currentEntry.users == 0 ? null : currentEntry;
        });
    }

    static int size() {
        return TABLE_CACHE.size();
    }

    static void clearForTest() {
        TABLE_CACHE.clear();
    }

    static final class TableCacheEntry {
        private final Table table;
        private final List<String> fieldNames;
        private int users = 1;

        TableCacheEntry(Table table, List<String> fieldNames) {
            this.table = table;
            this.fieldNames = Collections.unmodifiableList(new ArrayList<>(fieldNames));
        }

        Table table() {
            return table;
        }

        List<String> fieldNames() {
            return fieldNames;
        }
    }
}

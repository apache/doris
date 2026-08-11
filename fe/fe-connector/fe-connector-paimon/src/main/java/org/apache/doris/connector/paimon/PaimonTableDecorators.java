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

package org.apache.doris.connector.paimon;

import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;

/**
 * The one place that knows how Paimon stacks {@link DelegatedFileStoreTable} decorators on a loaded
 * table, and how far they may be peeled off.
 */
final class PaimonTableDecorators {

    private PaimonTableDecorators() {
    }

    /**
     * Peel the decorators Paimon may have stacked on top of the table, down to the fallback-branch
     * pair - the one layer that must stay on top, because that is what dispatches a read to the
     * right branch.
     *
     * <p>{@code PrivilegedCatalog#getTable} wraps whatever {@code FileStoreTableFactory} built into
     * a {@code PrivilegedFileStoreTable}, so with file based privileges enabled a
     * {@code scan.fallback-branch} table reaches Doris as {@code Privileged(FallbackRead(...))}.
     * Paimon itself never lets a decorator sit above the pair - {@code CatalogUtils#loadTable}
     * builds system tables straight over the {@code FileStoreTableFactory} result and only wraps
     * what it returns - and both Paimon and Doris dispatch on a direct {@code instanceof
     * FallbackReadFileStoreTable}, which looks straight past a decorator and silently falls back to
     * the delegated main branch alone.
     *
     * <p>The decorators are peeled off rather than re-applied, and what that costs differs by
     * caller. Rebuilding the table the BE gets loses nothing: the privilege wrapper only asserts on
     * {@code newScan()} / {@code newRead()}, which the FE has already run while planning, and a
     * plain (non fallback) table loses the wrapper the same way once it is rebuilt out of
     * fileIO / location / schema. Building a system table over the peeled base does drop that
     * assertion, but that is Paimon's own semantics rather than a relaxation of it:
     * {@code PrivilegedCatalog#getTable} wraps only a result that is a {@code FileStoreTable}, and
     * no system table is one, so {@code db.tbl$ro} never carries the decorator there either.
     */
    static FileStoreTable unwrapToFallbackOrBase(FileStoreTable dataTable) {
        FileStoreTable current = dataTable;
        while (current instanceof DelegatedFileStoreTable && !(current instanceof FallbackReadFileStoreTable)) {
            current = ((DelegatedFileStoreTable) current).wrapped();
        }
        return current;
    }
}

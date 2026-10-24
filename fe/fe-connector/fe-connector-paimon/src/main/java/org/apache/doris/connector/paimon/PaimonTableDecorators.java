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

import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

/**
 * The one place that knows how Paimon stacks {@link DelegatedFileStoreTable} decorators on a loaded
 * table, and how far they may be peeled off.
 */
final class PaimonTableDecorators {

    private PaimonTableDecorators() {
    }

    /**
     * Rebuild a fallback-branch pair around new branches. A chain table is a pair whose other branch is a
     * {@link ChainGroupReadTable} over its snapshot and delta branches: the group plans and reads the chain
     * itself, and it is the only pair Paimon lets one nest inside another, so it is rebuilt as a group.
     */
    static FileStoreTable rebuildFallbackPair(FallbackReadFileStoreTable pair, FileStoreTable wrapped,
            FileStoreTable other) {
        if (pair instanceof ChainGroupReadTable) {
            return new ChainGroupReadTable(wrapped, other);
        }
        return new FallbackReadFileStoreTable(wrapped, other, PaimonReaderOptions.isWrappedFirst(pair));
    }

    /**
     * Whether {@code table} reads as a chain table: a fallback pair whose other branch is a
     * {@link ChainGroupReadTable}. A branch of a chain table read on its own is a plain table.
     */
    static boolean isChainTable(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return false;
        }
        FileStoreTable base = unwrapToFallbackOrBase((FileStoreTable) table);
        return base instanceof FallbackReadFileStoreTable
                && ((FallbackReadFileStoreTable) base).other() instanceof ChainGroupReadTable;
    }

    /**
     * Peel the decorators Paimon may have stacked on top of the table, down to the fallback-branch
     * pair - the one layer that must stay on top, because that is what dispatches a read to the
     * right branch.
     */
    static FileStoreTable unwrapToFallbackOrBase(FileStoreTable dataTable) {
        FileStoreTable current = dataTable;
        while (current instanceof DelegatedFileStoreTable && !(current instanceof FallbackReadFileStoreTable)) {
            current = ((DelegatedFileStoreTable) current).wrapped();
        }
        return current;
    }
}

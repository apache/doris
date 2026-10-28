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

import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

/** Utilities for preserving Paimon's planning decorator invariants. */
public final class PaimonTableDecorators {

    private PaimonTableDecorators() {
    }

    public static FileStoreTable unwrapToFallbackOrBase(FileStoreTable table) {
        FileStoreTable current = table;
        // Fallback dispatch requires the pair to be the system wrapper's immediate child. Paimon
        // does not privilege-wrap system tables, so peeling privilege-only delegates matches its
        // catalog semantics while retaining the one decorator that changes read routing.
        while (current instanceof DelegatedFileStoreTable
                && !(current instanceof FallbackReadFileStoreTable)) {
            current = ((DelegatedFileStoreTable) current).wrapped();
        }
        return current;
    }

    static FileStoreTable rebuildFallbackPair(FallbackReadFileStoreTable pair, FileStoreTable wrapped,
            FileStoreTable other) {
        if (pair instanceof ChainGroupReadTable) {
            return new ChainGroupReadTable(wrapped, other);
        }
        return new FallbackReadFileStoreTable(wrapped, other, PaimonReaderOptions.isWrappedFirst(pair));
    }

    static Map<String, String> otherOptions(FallbackReadFileStoreTable pair, Map<String, String> options) {
        return new FallbackOptions(pair).rewriteOtherOptions(options);
    }

    private static final class FallbackOptions extends FallbackReadFileStoreTable {
        private FallbackOptions(FallbackReadFileStoreTable pair) {
            super(pair.wrapped(), pair.other(), PaimonReaderOptions.isWrappedFirst(pair));
        }

        @Override
        public Map<String, String> rewriteOtherOptions(Map<String, String> options) {
            return super.rewriteOtherOptions(options);
        }
    }

    static boolean isChainTable(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return false;
        }
        FileStoreTable unwrapped = unwrapToFallbackOrBase((FileStoreTable) table);
        return unwrapped instanceof FallbackReadFileStoreTable
                && ((FallbackReadFileStoreTable) unwrapped).other() instanceof ChainGroupReadTable;
    }
}

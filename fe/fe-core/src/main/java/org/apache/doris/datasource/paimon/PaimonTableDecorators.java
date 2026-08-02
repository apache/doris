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

import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;

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
}

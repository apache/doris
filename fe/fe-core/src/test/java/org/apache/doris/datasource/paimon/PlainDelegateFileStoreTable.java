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

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;

/**
 * Minimal non-fallback delegate used to exercise the delegate-peeling paths that the removed
 * Paimon privilege wrapper used to cover.
 */
final class PlainDelegateFileStoreTable extends DelegatedFileStoreTable {

    PlainDelegateFileStoreTable(FileStoreTable wrapped) {
        super(wrapped);
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new PlainDelegateFileStoreTable(wrapped().copy(dynamicOptions));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new PlainDelegateFileStoreTable(wrapped().copy(newTableSchema));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new PlainDelegateFileStoreTable(wrapped().copyWithoutTimeTravel(dynamicOptions));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new PlainDelegateFileStoreTable(wrapped().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new PlainDelegateFileStoreTable(wrapped().switchToBranch(branchName));
    }
}

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

package org.apache.doris.connector.api.handle;

import java.util.Set;

/**
 * Narrow opt-in capability for a {@link ConnectorTransaction} that supports the compaction
 * {@code rewrite_data_files} procedure (only iceberg today).
 *
 * <p>Kept OFF {@link ConnectorTransaction} so the shared transaction contract carries no source-specific
 * methods: the engine rewrite driver ({@code ConnectorRewriteDriver}) checks {@code instanceof
 * RewriteCapableTransaction} before it calls, turning "unsupported" from a runtime throw into a type
 * mismatch. A connector whose transaction is not rewrite-capable simply does not implement this.</p>
 */
public interface RewriteCapableTransaction {

    /**
     * Compaction rewrite ({@code rewrite_data_files}): registers the original data files this transaction
     * will atomically replace, by their RAW file paths. The engine rewrite driver hands the connector the
     * neutral {@code String} paths from its bin-packed groups (fe-core cannot traffic in connector-native
     * file objects across the wall); the connector resolves them back to its own file objects — e.g. by
     * re-deriving from the table at the transaction's pinned snapshot — and removes them at
     * {@code ConnectorTransaction#commit()}.
     *
     * @param dataFilePaths the raw paths of the source data files to replace
     */
    void registerRewriteSourceFiles(Set<String> dataFilePaths);

    /**
     * Compaction rewrite: the number of new compacted data files this transaction added, available only
     * AFTER {@code ConnectorTransaction#commit()} (the count is materialized from the BE-reported commit
     * fragments during commit). Feeds the procedure's {@code added_data_files_count} result column — the one
     * rewrite-result statistic the engine driver cannot compute from its planning groups.
     */
    int getRewriteAddedDataFilesCount();
}

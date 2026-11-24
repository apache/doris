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

package org.apache.doris.connector.hms;

/**
 * Doris addition implemented by the vendored HMS client: exposes the RAW (pre-{@code MetaStoreFilterHook})
 * page size of {@code get_partitions_by_filter} alongside the usual hook-filtered partitions.
 *
 * <p>Callers that must not silently drop matching partitions (Doris's direct-HMS partition pruning) use this
 * instead of {@code IMetaStoreClient#listPartitionsByFilter}: the hook-filtered list alone cannot tell a
 * truncated page from a complete one when the hook hides an entry. Implementations that do not opt in are
 * unaffected.</p>
 */
public interface HmsRawPartitionFilterPageSource {

    /**
     * Lists the hook-filtered partitions matching an HMS filter expression, plus the raw pre-hook page size.
     *
     * @param dbName database name
     * @param tableName table name
     * @param filter HMS {@code get_partitions_by_filter} expression
     * @param maxParts upper bound on the RAW metastore page
     * @return the hook-filtered partitions and the raw page size
     */
    HmsRawPartitionFilterPage listPartitionsByFilterRawPage(String dbName, String tableName, String filter,
            int maxParts) throws Exception;
}

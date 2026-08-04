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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.DorisConnectorException;

/**
 * Thrown when listing ONE partition directory fails ({@code FileSystem.listStatus} raised an
 * {@code IOException}: a missing / unreadable / transiently-failing directory). This is a <b>local</b>
 * failure that the scan path tolerates by skipping that partition with a warning — the pre-cache resilience
 * (legacy {@code HiveScanPlanProvider.listAndSplitFiles} caught the {@code listStatus} {@code IOException}
 * and skipped the partition).
 *
 * <p>It is deliberately a distinct subtype of {@link DorisConnectorException} so the scan path can catch
 * <em>only</em> this (skip) while letting a plain {@link DorisConnectorException} — used for a <b>systemic</b>
 * filesystem-resolution failure ({@code FileSystem.get}: unknown scheme, bad credentials/endpoint, which
 * affects every partition of the table) — propagate and fail the query loud, exactly as legacy did before the
 * listing cache folded the two failure modes together. Never cached (the cache loader throws, and
 * {@code MetaCacheEntry} never caches a failed load).</p>
 */
public class HiveDirectoryListingException extends DorisConnectorException {

    public HiveDirectoryListingException(String message, Throwable cause) {
        super(message, cause);
    }
}

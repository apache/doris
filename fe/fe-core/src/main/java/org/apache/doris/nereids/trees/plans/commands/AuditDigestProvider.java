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

package org.apache.doris.nereids.trees.plans.commands;

import java.util.Optional;

/**
 * Provides the normalized digest written to the audit log's sql_digest field for statements that
 * are not classified as queries ({@code is_query=false}). Keeping this separate from
 * {@code Queriable#toDigest} lets DML statements carry a fingerprint without changing their query
 * classification or the query metrics they feed.
 */
public interface AuditDigestProvider {
    /**
     * Returns the digest to record, or empty when the statement has no query semantics to
     * fingerprint (for example {@code INSERT ... VALUES}).
     */
    Optional<String> getAuditDigest();
}

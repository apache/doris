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

package org.apache.doris.authorization;

/**
 * The kinds of object an authorization decision can be about.
 *
 * <p>The list is exhaustive by construction: it has one constant per question the engine can ask, so an
 * implementation that switches over it and rejects the default branch cannot be silently bypassed by a new
 * kind of object appearing.</p>
 *
 * <p>The cloud kinds are separate from {@link #RESOURCE} and {@link #STORAGE_VAULT} rather than folded into
 * them because they are separate questions today, answered against different privilege tables and with an
 * extra fallback of their own. Folding them would change who is allowed in.</p>
 */
public enum ResourceKind {
    /** The cluster as a whole. */
    GLOBAL,
    CATALOG,
    DATABASE,
    TABLE,
    /** Named columns of one table. */
    COLUMNS,
    /** A resource, e.g. an ODBC or S3 resource. */
    RESOURCE,
    WORKLOAD_GROUP,
    STORAGE_VAULT,
    /** A cloud resource that is none of the kinds below. */
    CLOUD_GENERAL,
    /** A compute group, historically named "cluster". */
    CLOUD_COMPUTE_GROUP,
    CLOUD_STAGE,
    /** A storage vault reached through the cloud privilege path, distinct from {@link #STORAGE_VAULT}. */
    CLOUD_STORAGE_VAULT
}

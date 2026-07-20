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

import org.apache.doris.connector.api.Connector;

/**
 * The embedded sibling connector that owns a foreign (iceberg/hudi-on-HMS) table, paired with its stable owner
 * label. Produced by the 3-way ownsHandle dispatch ({@code HiveConnector.resolveSiblingOwnerLabeled}) so the
 * per-handle gateway seams learn WHICH sibling owns a handle AND its label in a single peek (no force-build, no
 * identity comparison).
 *
 * <p>The label is the single source of truth for the per-statement metadata funnel key
 * ({@code "metadata:" + catalogId + ":" + label}) that lets a statement reuse ONE sibling metadata across all its
 * forwards. It MUST be identical on the by-TYPE divert path ({@code getTableHandle}, which has no handle yet and
 * asks a sibling by type) and the by-HANDLE forward path (every per-handle guard-and-forward) for the same owner
 * — otherwise the two paths would memoize two sibling metadata instances under two keys within one statement.
 * Both routes therefore key off {@link #ICEBERG_LABEL} / {@link #HUDI_LABEL} here.
 *
 * <p>The connector is held ONLY as the parent-first {@link Connector} interface — never cast (its concrete
 * iceberg/hudi types are invisible across the plugin classloader split; a cast would CCE).
 */
final class SiblingOwner {

    /** Owner label for the embedded iceberg sibling — the funnel-key discriminator, shared by both routes. */
    static final String ICEBERG_LABEL = "iceberg";
    /** Owner label for the embedded hudi sibling — the funnel-key discriminator, shared by both routes. */
    static final String HUDI_LABEL = "hudi";

    private final Connector connector;
    private final String label;

    SiblingOwner(Connector connector, String label) {
        this.connector = connector;
        this.label = label;
    }

    Connector connector() {
        return connector;
    }

    String label() {
        return label;
    }
}

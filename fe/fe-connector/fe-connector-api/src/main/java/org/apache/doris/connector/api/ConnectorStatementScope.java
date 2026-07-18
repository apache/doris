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

package org.apache.doris.connector.api;

import java.util.function.Supplier;

/**
 * A per-statement memoization arena, living exactly as long as one SQL statement (read + write).
 *
 * <p>It lets a connector load a table (and derive per-statement state) once and share that single
 * object across every resolver in the statement — read metadata, scan planning, write shaping,
 * begin-write. Reached from a connector via {@link ConnectorSession#getStatementScope()}.</p>
 *
 * <p>Neutral SPI: the engine owns the physical home (the scope is hung on the engine's per-statement
 * context) and the connector stores its own connector-typed values under string keys as opaque
 * {@code Object}s — fe-core never sees a connector type. Values are session-bound and reclaimed with
 * the statement, never promoted into any cross-session / cross-identity structure.</p>
 *
 * <p>{@link #NONE} is the off-context default (no live statement: offline planning, tests, and any
 * {@link ConnectorSession} that does not override {@link ConnectorSession#getStatementScope()}). It
 * never memoizes, so every call runs the loader — byte-identical to loading every time.</p>
 */
public interface ConnectorStatementScope {

    /**
     * Returns the value cached under {@code key}, computing it with {@code loader} on first access and
     * caching it for the rest of the statement. Within one statement the same key returns the same
     * instance to every caller; under {@link #NONE} the loader runs on every call.
     */
    <T> T computeIfAbsent(String key, Supplier<T> loader);

    /** The no-op scope: never caches; each call invokes the loader (offline / no-context / tests). */
    ConnectorStatementScope NONE = new ConnectorStatementScope() {
        @Override
        public <T> T computeIfAbsent(String key, Supplier<T> loader) {
            return loader.get();
        }
    };
}

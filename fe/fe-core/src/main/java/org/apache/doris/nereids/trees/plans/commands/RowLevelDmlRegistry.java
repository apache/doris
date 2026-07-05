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

import org.apache.doris.catalog.TableIf;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Registry of {@link RowLevelDmlTransform}s. The dispatching DML commands consult this instead of testing the
 * target table type, so the reverse {@code instanceof} dispatch is consolidated here.
 *
 * <p>Explicit static registration (no {@code ServiceLoader}) — avoids the thread-context-classloader pitfalls
 * seen with SPI loaders. Today the single entry is {@link IcebergRowLevelDmlTransform}, whose {@code handles}
 * is {@code instanceof IcebergExternalTable}; at P6.6 cutover the predicate can become a capability check.</p>
 */
public final class RowLevelDmlRegistry {

    private static final List<RowLevelDmlTransform> TRANSFORMS =
            ImmutableList.of(new IcebergRowLevelDmlTransform());

    private RowLevelDmlRegistry() {
    }

    /** Returns the first transform that handles the table, or empty (the OLAP/native path). */
    public static Optional<RowLevelDmlTransform> find(TableIf table) {
        if (table == null) {
            return Optional.empty();
        }
        for (RowLevelDmlTransform transform : TRANSFORMS) {
            if (transform.handles(table)) {
                return Optional.of(transform);
            }
        }
        return Optional.empty();
    }
}

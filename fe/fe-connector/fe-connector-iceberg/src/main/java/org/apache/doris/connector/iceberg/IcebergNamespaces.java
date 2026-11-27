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

package org.apache.doris.connector.iceberg;

import com.google.common.base.Splitter;
import org.apache.iceberg.catalog.Namespace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Maps between Doris database names and the Iceberg namespaces used by catalog lookups. */
final class IcebergNamespaces {
    private IcebergNamespaces() {
    }

    static Namespace fromDatabaseName(String database, Optional<String> externalCatalogName) {
        List<String> levels = new ArrayList<>(Splitter.on('.').omitEmptyStrings().trimResults().splitToList(database));
        externalCatalogName.ifPresent(levels::add);
        return Namespace.of(levels.toArray(new String[0]));
    }

    static Optional<String> toDatabaseName(List<String> levels, Optional<String> externalCatalogName) {
        int dbLevels = levels.size() - (externalCatalogName.isPresent() ? 1 : 0);
        if (dbLevels <= 0) {
            return Optional.empty();
        }
        String database = String.join(".", levels.subList(0, dbLevels));
        // A literal dot, whitespace, or a different catalog root can otherwise select another namespace.
        Namespace resolved = fromDatabaseName(database, externalCatalogName);
        return levels.equals(Arrays.asList(resolved.levels())) ? Optional.of(database) : Optional.empty();
    }
}

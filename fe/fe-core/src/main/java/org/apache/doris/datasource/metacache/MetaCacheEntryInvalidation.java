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

package org.apache.doris.datasource.metacache;

import org.apache.doris.datasource.NameMapping;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Entry-level invalidation metadata used by {@link AbstractExternalMetaCache}.
 */
public final class MetaCacheEntryInvalidation<K> {
    @FunctionalInterface
    public interface PartitionPredicateFactory<K> {
        Predicate<K> create(String dbName, String tableName, List<String> partitions);
    }

    private static final MetaCacheEntryInvalidation<?> NONE = new MetaCacheEntryInvalidation<>(null, null, null);

    @Nullable
    private final Function<String, Predicate<K>> dbPredicateFactory;
    @Nullable
    private final BiFunction<String, String, Predicate<K>> tablePredicateFactory;
    @Nullable
    private final PartitionPredicateFactory<K> partitionPredicateFactory;

    private MetaCacheEntryInvalidation(
            @Nullable Function<String, Predicate<K>> dbPredicateFactory,
            @Nullable BiFunction<String, String, Predicate<K>> tablePredicateFactory,
            @Nullable PartitionPredicateFactory<K> partitionPredicateFactory) {
        this.dbPredicateFactory = dbPredicateFactory;
        this.tablePredicateFactory = tablePredicateFactory;
        this.partitionPredicateFactory = partitionPredicateFactory;
    }

    @SuppressWarnings("unchecked")
    public static <K> MetaCacheEntryInvalidation<K> none() {
        return (MetaCacheEntryInvalidation<K>) NONE;
    }

    public static <K> MetaCacheEntryInvalidation<K> forNameMapping(Function<K, NameMapping> nameMappingExtractor) {
        Objects.requireNonNull(nameMappingExtractor, "nameMappingExtractor");
        return forTableIdentity(
                key -> nameMappingExtractor.apply(key).getLocalDbName(),
                key -> nameMappingExtractor.apply(key).getLocalTblName());
    }

    public static <K> MetaCacheEntryInvalidation<K> forTableIdentity(
            Function<K, String> dbNameExtractor, Function<K, String> tableNameExtractor) {
        Objects.requireNonNull(dbNameExtractor, "dbNameExtractor");
        Objects.requireNonNull(tableNameExtractor, "tableNameExtractor");
        return new MetaCacheEntryInvalidation<>(
                dbName -> key -> dbNameExtractor.apply(key).equals(dbName),
                (dbName, tableName) -> key -> dbNameExtractor.apply(key).equals(dbName)
                        && tableNameExtractor.apply(key).equals(tableName),
                (dbName, tableName, partitions) -> key -> dbNameExtractor.apply(key).equals(dbName)
                        && tableNameExtractor.apply(key).equals(tableName));
    }

    @Nullable
    Predicate<K> dbPredicate(String dbName) {
        return dbPredicateFactory == null ? null : dbPredicateFactory.apply(dbName);
    }

    @Nullable
    Predicate<K> tablePredicate(String dbName, String tableName) {
        return tablePredicateFactory == null ? null : tablePredicateFactory.apply(dbName, tableName);
    }

    @Nullable
    Predicate<K> partitionPredicate(String dbName, String tableName, List<String> partitions) {
        return partitionPredicateFactory == null
                ? null
                : partitionPredicateFactory.create(dbName, tableName, partitions);
    }
}

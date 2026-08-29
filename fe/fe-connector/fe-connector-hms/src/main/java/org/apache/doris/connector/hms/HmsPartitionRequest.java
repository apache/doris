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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable logical input for one HMS partition-object request. */
final class HmsPartitionRequest {

    private final String dbName;
    private final String tableName;
    private final List<HmsPartitionIdentity.ParsedPartitionName> partitions;

    private HmsPartitionRequest(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.partitions = parsePartitions(builder.partitionNames);
    }

    static Builder builder() {
        return new Builder();
    }

    String getDbName() {
        return dbName;
    }

    String getTableName() {
        return tableName;
    }

    List<HmsPartitionIdentity.ParsedPartitionName> getPartitions() {
        return partitions;
    }

    static final class Builder {
        private String dbName;
        private String tableName;
        private List<String> partitionNames;

        private Builder() {
        }

        Builder database(String dbName) {
            this.dbName = dbName;
            return this;
        }

        Builder table(String tableName) {
            this.tableName = tableName;
            return this;
        }

        Builder partitionNames(List<String> partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        HmsPartitionRequest build() {
            requireName(dbName, "database");
            requireName(tableName, "table");
            Objects.requireNonNull(partitionNames, "partitionNames");
            return new HmsPartitionRequest(this);
        }

        private static void requireName(String value, String field) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(field + " must not be empty");
            }
        }
    }

    private static List<HmsPartitionIdentity.ParsedPartitionName> parsePartitions(List<String> names) {
        List<HmsPartitionIdentity.ParsedPartitionName> parsedPartitions = new ArrayList<>(names.size());
        Set<List<String>> identities = new HashSet<>();
        List<String> partitionKeys = null;
        for (String name : names) {
            HmsPartitionIdentity.ParsedPartitionName parsed = HmsPartitionIdentity.parse(name);
            if (partitionKeys == null) {
                partitionKeys = parsed.getKeys();
            } else if (!partitionKeys.equals(parsed.getKeys())) {
                throw new IllegalArgumentException("inconsistent partition keys in request: " + name);
            }
            if (!identities.add(parsed.getValues())) {
                throw new IllegalArgumentException("duplicate partition identity in request: " + name);
            }
            parsedPartitions.add(parsed);
        }
        return Collections.unmodifiableList(parsedPartitions);
    }
}

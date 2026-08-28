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

import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorMetadataAccessSource;
import org.apache.doris.connector.spi.ConnectorSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class HmsPartitionRequest {

    private final String dbName;
    private final String tableName;
    private final List<String> partitionNames;
    private final List<HmsPartitionIdentity.ParsedPartitionName> partitions;
    private final ConnectorMetadataAccessSource source;
    private final ConnectorMetadataAccessObserver metadataAccessObserver;

    private HmsPartitionRequest(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.partitions = builder.partitions == null
                ? parsePartitions(builder.partitionNames)
                : Collections.unmodifiableList(new ArrayList<>(builder.partitions));
        List<String> names = new ArrayList<>(partitions.size());
        for (HmsPartitionIdentity.ParsedPartitionName partition : partitions) {
            names.add(partition.getName());
        }
        this.partitionNames = Collections.unmodifiableList(names);
        this.source = builder.source;
        this.metadataAccessObserver = builder.metadataAccessObserver;
    }

    static HmsPartitionRequest from(ConnectorSession session, ConnectorMetadataAccessSource source,
            String dbName, String tableName, List<String> partitionNames) {
        return builder()
                .database(dbName)
                .table(tableName)
                .partitionNames(partitionNames)
                .source(source)
                .metadataAccessObserver(session == null
                        ? ConnectorMetadataAccessObserver.NOOP : session.getMetadataAccessObserver())
                .build();
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

    List<String> getPartitionNames() {
        return partitionNames;
    }

    List<HmsPartitionIdentity.ParsedPartitionName> getPartitions() {
        return partitions;
    }

    ConnectorMetadataAccessSource getSource() {
        return source;
    }

    ConnectorMetadataAccessObserver getMetadataAccessObserver() {
        return metadataAccessObserver;
    }

    static final class Builder {
        private String dbName;
        private String tableName;
        private List<String> partitionNames;
        private List<HmsPartitionIdentity.ParsedPartitionName> partitions;
        private ConnectorMetadataAccessSource source = ConnectorMetadataAccessSource.UNKNOWN;
        private ConnectorMetadataAccessObserver metadataAccessObserver = ConnectorMetadataAccessObserver.NOOP;

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

        Builder partitions(List<HmsPartitionIdentity.ParsedPartitionName> partitions) {
            this.partitions = partitions;
            return this;
        }

        Builder source(ConnectorMetadataAccessSource source) {
            this.source = source;
            return this;
        }

        Builder metadataAccessObserver(ConnectorMetadataAccessObserver metadataAccessObserver) {
            this.metadataAccessObserver = metadataAccessObserver;
            return this;
        }

        HmsPartitionRequest build() {
            requireName(dbName, "database");
            requireName(tableName, "table");
            if (partitions == null) {
                Objects.requireNonNull(partitionNames, "partitionNames");
            }
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(metadataAccessObserver, "metadataAccessObserver");
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
        for (int i = 0; i < names.size(); i++) {
            HmsPartitionIdentity.ParsedPartitionName parsed = HmsPartitionIdentity.parse(names.get(i));
            if (partitionKeys == null) {
                partitionKeys = parsed.getKeys();
            } else if (!partitionKeys.equals(parsed.getKeys())) {
                throw new IllegalArgumentException("inconsistent partition keys in request: " + parsed.getName());
            }
            if (!identities.add(parsed.getValues())) {
                throw new IllegalArgumentException("duplicate partition identity in request: " + parsed.getName());
            }
            parsedPartitions.add(parsed);
        }
        return Collections.unmodifiableList(parsedPartitions);
    }

}

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

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Hive partition to be created, bundled with the statistics to stamp on it.
 *
 * <p>SPI-clean port of fe-core's {@code datasource.hive.HivePartitionWithStatistics} plus the
 * storage-descriptor fields of {@code HivePartition} (a fe-core type), flattened so the DTO depends
 * only on connector-api / JDK / hive-metastore-api types. {@link HmsWriteConverter#toHivePartitions}
 * turns a list of these into Hive metastore {@code Partition} objects for
 * {@link HmsClient#addPartitions}.</p>
 */
public final class HmsPartitionWithStatistics {

    private final String name;
    private final List<String> partitionValues;
    private final String location;
    private final List<FieldSchema> columns;
    private final String inputFormat;
    private final String outputFormat;
    private final String serde;
    private final Map<String, String> parameters;
    private final HmsPartitionStatistics statistics;

    private HmsPartitionWithStatistics(Builder builder) {
        this.name = builder.name;
        this.partitionValues = builder.partitionValues == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(builder.partitionValues);
        this.location = builder.location;
        this.columns = builder.columns == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(builder.columns);
        this.inputFormat = builder.inputFormat;
        this.outputFormat = builder.outputFormat;
        this.serde = builder.serde;
        this.parameters = builder.parameters == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(builder.parameters);
        this.statistics = builder.statistics == null
                ? HmsPartitionStatistics.EMPTY
                : builder.statistics;
    }

    /** Partition name (e.g. "dt=2024-01-01"). Used by the committer for tracking/logging. */
    public String getName() {
        return name;
    }

    /** Partition column values in declaration order. */
    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public String getLocation() {
        return location;
    }

    public List<FieldSchema> getColumns() {
        return columns;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public String getSerde() {
        return serde;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public HmsPartitionStatistics getStatistics() {
        return statistics;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for HmsPartitionWithStatistics.
     */
    public static final class Builder {
        private String name;
        private List<String> partitionValues;
        private String location;
        private List<FieldSchema> columns;
        private String inputFormat;
        private String outputFormat;
        private String serde;
        private Map<String, String> parameters;
        private HmsPartitionStatistics statistics;

        private Builder() {
        }

        public Builder name(String val) {
            this.name = val;
            return this;
        }

        public Builder partitionValues(List<String> val) {
            this.partitionValues = val;
            return this;
        }

        public Builder location(String val) {
            this.location = val;
            return this;
        }

        public Builder columns(List<FieldSchema> val) {
            this.columns = val;
            return this;
        }

        public Builder inputFormat(String val) {
            this.inputFormat = val;
            return this;
        }

        public Builder outputFormat(String val) {
            this.outputFormat = val;
            return this;
        }

        public Builder serde(String val) {
            this.serde = val;
            return this;
        }

        public Builder parameters(Map<String, String> val) {
            this.parameters = val;
            return this;
        }

        public Builder statistics(HmsPartitionStatistics val) {
            this.statistics = val;
            return this;
        }

        public HmsPartitionWithStatistics build() {
            return new HmsPartitionWithStatistics(this);
        }
    }
}

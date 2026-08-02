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

package org.apache.doris.connector.fluss;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the {@link TableInfo} a fluss cluster would hand back, for tests that do not start one.
 *
 * <p>It goes through fluss's own {@link TableDescriptor} and {@link TableInfo#of} rather than
 * assembling a {@link TableInfo} field by field, so a fixture cannot describe a table fluss would
 * refuse to create (a partition key outside the primary key, a missing bucket count) and quietly make
 * the connector look correct on input it will never see.
 */
final class FlussTestTables {

    /** Ids a fixture does not care about; a real cluster assigns its own. */
    private static final long TABLE_ID = 1L;
    private static final int SCHEMA_ID = 1;

    private FlussTestTables() {
    }

    static Builder builder(TablePath tablePath) {
        return new Builder(tablePath);
    }

    static final class Builder {

        private final TablePath tablePath;
        private final Schema.Builder schema = Schema.newBuilder();
        private final Map<String, String> properties = new LinkedHashMap<>();
        private final List<String> partitionKeys = new ArrayList<>();
        private List<String> bucketKeys = new ArrayList<>();
        private int bucketCount = 1;
        private String comment;

        private Builder(TablePath tablePath) {
            this.tablePath = tablePath;
        }

        Builder column(String name, DataType type) {
            schema.column(name, type);
            return this;
        }

        Builder column(String name, DataType type, String columnComment) {
            schema.column(name, type).withComment(columnComment);
            return this;
        }

        Builder primaryKey(String... columns) {
            schema.primaryKey(columns);
            return this;
        }

        Builder partitionedBy(String... columns) {
            partitionKeys.addAll(Arrays.asList(columns));
            return this;
        }

        Builder buckets(int count, String... keys) {
            this.bucketCount = count;
            this.bucketKeys = new ArrayList<>(Arrays.asList(keys));
            return this;
        }

        Builder property(String key, String value) {
            properties.put(key, value);
            return this;
        }

        Builder comment(String tableComment) {
            this.comment = tableComment;
            return this;
        }

        TableInfo build() {
            TableDescriptor descriptor = TableDescriptor.builder()
                    .schema(schema.build())
                    .partitionedBy(partitionKeys)
                    .distributedBy(bucketCount, bucketKeys)
                    .properties(properties)
                    .comment(comment)
                    .build();
            return TableInfo.of(tablePath, TABLE_ID, SCHEMA_ID, descriptor, null, 0L, 0L);
        }
    }
}

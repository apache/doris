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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionField;
import org.apache.doris.connector.spi.ddl.ConnectorPartitionSpec;

import java.util.ArrayList;
import java.util.List;

/** Checks every CREATE clause before the adapter performs any external mutation. */
final class DeltaCreateTableValidator {
    private DeltaCreateTableValidator() {
    }

    static List<String> partitionColumns(ConnectorCreateTableRequest request) {
        if (request.getBucketSpec() != null || !request.getSortOrder().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Native Delta CREATE TABLE does not support bucket or write-sort specifications");
        }
        ConnectorPartitionSpec spec = request.getPartitionSpec();
        if (spec == null) {
            return List.of();
        }
        // Doris PARTITION BY LIST(col) without value definitions is the identity-partition spelling used
        // by external catalogs; explicit LIST values and RANGE/transform semantics cannot be discarded.
        if ((spec.getStyle() != ConnectorPartitionSpec.Style.IDENTITY
                && spec.getStyle() != ConnectorPartitionSpec.Style.LIST) || spec.hasExplicitPartitionValues()) {
            throw new UnsupportedOperationException(
                    "Native Delta CREATE TABLE supports identity partition columns only");
        }
        List<String> columns = new ArrayList<>();
        for (ConnectorPartitionField field : spec.getFields()) {
            if (!"identity".equalsIgnoreCase(field.getTransform()) || !field.getTransformArgs().isEmpty()) {
                throw new UnsupportedOperationException(
                        "Native Delta CREATE TABLE supports identity partition columns only");
            }
            columns.add(field.getColumnName());
        }
        return List.copyOf(columns);
    }
}

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

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the hive CREATE TABLE validation moved off fe-core {@code CreateTableInfo} / {@code PartitionTableInfo}:
 * hive rejects a {@code NOT NULL} column and enforces the hive external partition rules (column existence, no
 * floating-point / complex partition columns, the {@code allow_partition_column_nullable} rule, no duplicate
 * partition columns, and the schema-placement rules).
 *
 * <p><b>WHY this matters (Rule 9):</b> a hive metastore table cannot enforce nullability and its partition columns
 * must obey hive layout rules; fe-core used to reject violations at analysis time. After delegating create to the
 * connector, these checks must run connector-side or the violations reach the metastore (or are silently accepted).
 * These tests lock the connector-side checks in.</p>
 *
 * <p>The validators are package-private (reached only via {@code createTable} in production, which needs a live HMS
 * client); the test constructs the metadata offline with null client/context (the validators touch only the request
 * plus the nullable flag), mirroring {@code MaxComputeValidateColumnsTest}.</p>
 */
public class HiveCreateTableValidationTest {

    private HiveConnectorMetadata metadata() {
        return new HiveConnectorMetadata(null, null, null);
    }

    private ConnectorColumn col(String name, String type, boolean nullable) {
        return new ConnectorColumn(name, ConnectorType.of(type), "", nullable, null);
    }

    private ConnectorCreateTableRequest request(List<ConnectorColumn> columns) {
        return ConnectorCreateTableRequest.builder().dbName("db").tableName("t").columns(columns).build();
    }

    // ---- NOT NULL columns ----

    @Test
    public void notNullColumnIsRejected() {
        ConnectorCreateTableRequest request = request(Collections.singletonList(col("id", "INT", false)));
        // WHY: hive cannot enforce column nullability; fe-core rejected NOT NULL. MUTATION: dropping the check
        // lets a NOT NULL column reach the metastore where the constraint is meaningless.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateColumns(request));
        Assertions.assertTrue(ex.getMessage().contains("hive catalog doesn't support column with 'NOT NULL'."),
                "must reproduce the former fe-core NOT NULL message verbatim");
    }

    @Test
    public void nullableColumnsPass() {
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("v", "STRING", true)));
        // WHY: guards against over-rejection -- nullable columns (the hive norm) must validate.
        Assertions.assertDoesNotThrow(() -> metadata().validateColumns(request));
    }

    // ---- partition rules ----

    @Test
    public void partitionKeyMustExist() {
        ConnectorCreateTableRequest request = request(Collections.singletonList(col("id", "INT", true)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Collections.singletonList("missing")));
        Assertions.assertTrue(ex.getMessage().contains("partition key missing is not exists"),
                "must reproduce the former fe-core missing-partition-key message");
    }

    @Test
    public void floatingPointPartitionColumnIsRejected() {
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("f", "FLOAT", true)));
        // WHY: hive partitions map to directory names; a float partition column is invalid. MUTATION: dropping
        // the float check lets an invalid partition type through.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Collections.singletonList("f")));
        Assertions.assertTrue(ex.getMessage().contains("Floating point type column can not be partition column"),
                "must reproduce the former fe-core float message");
    }

    @Test
    public void complexPartitionColumnIsRejected() {
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("a", "ARRAY", true)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Collections.singletonList("a")));
        Assertions.assertTrue(ex.getMessage().contains("Complex type column can't be partition column"),
                "must reproduce the former fe-core complex-type message");
    }

    @Test
    public void nullablePartitionColumnRejectedWhenSessionVarOff() {
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("dt", "STRING", true)));
        // WHY (Rule 9): with allow_partition_column_nullable OFF a nullable partition column must be rejected,
        // reproducing the session-var-gated fe-core check. MUTATION: ignoring the flag lets it through.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, false, Collections.singletonList("dt")));
        Assertions.assertTrue(
                ex.getMessage().contains("The partition column must be NOT NULL with allow_partition_column_nullable"),
                "must reproduce the former fe-core nullable-partition message");
    }

    @Test
    public void duplicatePartitionColumnIsRejected() {
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("a", "INT", true)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Arrays.asList("a", "a")));
        Assertions.assertTrue(ex.getMessage().contains("Duplicated partition column a"),
                "must reproduce the former fe-core duplicate-partition message");
    }

    @Test
    public void allColumnsAsPartitionIsRejected() {
        ConnectorCreateTableRequest request = request(Collections.singletonList(col("dt", "STRING", true)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Collections.singletonList("dt")));
        Assertions.assertTrue(ex.getMessage().contains("Cannot set all columns as partitioning columns."),
                "must reproduce the former fe-core all-columns message");
    }

    @Test
    public void partitionFieldMustBeAtEndOfSchema() {
        // dt (the partition) sits BEFORE id in the schema -> hive requires partitions at the end.
        ConnectorCreateTableRequest request = request(Arrays.asList(col("dt", "STRING", true), col("id", "INT", true)));
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validatePartition(request, true, Collections.singletonList("dt")));
        Assertions.assertTrue(ex.getMessage().contains("The partition field must be at the end of the schema."),
                "must reproduce the former fe-core schema-placement message");
    }

    @Test
    public void validPartitionPasses() {
        // id data column then dt partition at the end: exists, string (allowed for external), nullable-ok.
        ConnectorCreateTableRequest request = request(Arrays.asList(col("id", "INT", true), col("dt", "STRING", true)));
        // WHY: guards against over-rejection -- a well-formed hive partitioned create must validate.
        Assertions.assertDoesNotThrow(() -> metadata().validatePartition(request, true,
                Collections.singletonList("dt")));
    }
}

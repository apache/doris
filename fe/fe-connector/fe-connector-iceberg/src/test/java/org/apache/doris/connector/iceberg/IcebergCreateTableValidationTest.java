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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.ConnectorSortField;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Pins the iceberg CREATE TABLE validation moved off fe-core {@code CreateTableInfo}: iceberg rejects a
 * {@code DISTRIBUTE BY} clause and validates the {@code ORDER BY} write sort order (column existence, sortable
 * type, no duplicates).
 *
 * <p><b>WHY this matters (Rule 9):</b> these per-source DDL rules used to live in fe-core; after moving the sort
 * validation connector-side, iceberg's {@code createTable} only mapped the sort order into an iceberg SortOrder
 * without re-checking it, so a bad sort column (missing / metric-only / duplicate) would either surface an obscure
 * downstream schema-build error or, for {@code DISTRIBUTE BY}, be silently ignored. These tests lock the
 * connector-side checks in.</p>
 *
 * <p>The validation methods are package-private (reached only via {@code createTable} in production, which needs a
 * live iceberg catalog); this test constructs the metadata offline with null catalog/context (the validators touch
 * only the request) and calls them directly — the same offline idiom as {@code MaxComputeValidateColumnsTest}.</p>
 *
 * <p>Also covers the other request field {@code createTable} resolves offline: folding a {@code COMMENT} clause
 * into the {@code comment} table property (see {@code applyCreateTableComment}).</p>
 */
public class IcebergCreateTableValidationTest {

    private IcebergConnectorMetadata metadata() {
        return new IcebergConnectorMetadata(null, null, null);
    }

    private ConnectorColumn col(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), "", true, null);
    }

    @Test
    public void distributeByIsRejected() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(col("id", "INT")))
                .bucketSpec(new ConnectorBucketSpec(Collections.singletonList("id"), 4, "doris_default"))
                .build();

        // WHY: iceberg has no DISTRIBUTE BY (buckets go in PARTITIONED BY via bucket(n, col)); fe-core used to
        // reject it. MUTATION: dropping the `if (getBucketSpec() != null) throw` makes this go red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().rejectDistribution(request));
        Assertions.assertTrue(ex.getMessage().contains("Iceberg doesn't support 'DISTRIBUTE BY'"),
                "must reproduce the former fe-core DISTRIBUTE BY message");
    }

    @Test
    public void noDistributeByPasses() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(col("id", "INT")))
                .build();
        // WHY: guards against over-rejection -- a create with no DISTRIBUTE BY must pass.
        Assertions.assertDoesNotThrow(() -> metadata().rejectDistribution(request));
    }

    @Test
    public void sortOrderColumnMustExist() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(col("id", "INT")))
                .sortOrder(Collections.singletonList(new ConnectorSortField("missing", true, false)))
                .build();

        // WHY: a sort column absent from the schema is a user error; fe-core rejected it. Existence match is
        // case-insensitive (mirrors the former CASE_INSENSITIVE_ORDER map). MUTATION: dropping the existence
        // check lets a bad ORDER BY reach the schema builder with an obscure error.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateSortOrder(request));
        Assertions.assertTrue(ex.getMessage().contains("does not exist in table"),
                "must reproduce the former fe-core existence message (e2e asserts this substring)");
    }

    @Test
    public void duplicateSortOrderColumnIsRejected() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Collections.singletonList(col("id", "INT")))
                .sortOrder(Arrays.asList(
                        new ConnectorSortField("id", true, false),
                        new ConnectorSortField("ID", false, false)))
                .build();

        // WHY: a duplicate sort column (case-insensitive) is a user error fe-core rejected. MUTATION: dropping
        // the duplicate check silently keeps the last occurrence.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().validateSortOrder(request));
        Assertions.assertTrue(ex.getMessage().contains("Duplicate sort order column"),
                "must reproduce the former fe-core duplicate message (e2e asserts this substring)");
    }

    @Test
    public void validSortOrderPasses() {
        ConnectorCreateTableRequest request = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Arrays.asList(col("id", "INT"), col("name", "STRING")))
                .sortOrder(Collections.singletonList(new ConnectorSortField("name", true, false)))
                .build();
        // WHY: guards against over-rejection -- a valid sort column must pass.
        Assertions.assertDoesNotThrow(() -> metadata().validateSortOrder(request));
    }

    // ---------- applyCreateTableComment ----------
    //
    // WHY this matters: the COMMENT clause and the PROPERTIES map are two DISTINCT nereids fields on the
    // create request. Legacy performCreateTable forwarded only PROPERTIES to iceberg, so `CREATE TABLE ...
    // COMMENT 'x'` persisted nothing and every comment reader (SHOW CREATE TABLE, TABLE_COMMENT, SHOW TABLE
    // STATUS) came back blank -- through a dedicated iceberg catalog and through an HMS gateway alike.

    @Test
    public void commentClauseIsFoldedIntoTableProperties() {
        Map<String, String> props = new HashMap<>();
        IcebergConnectorMetadata.applyCreateTableComment(props, "set through the COMMENT clause");
        Assertions.assertEquals("set through the COMMENT clause", props.get("comment"));
    }

    @Test
    public void explicitCommentPropertyWinsOverTheClause() {
        Map<String, String> props = new HashMap<>();
        props.put("comment", "from PROPERTIES");
        IcebergConnectorMetadata.applyCreateTableComment(props, "from the COMMENT clause");
        // WHY: legacy honored PROPERTIES("comment"=...) and nothing else; that behavior must survive intact,
        // so the clause is a fallback only.
        Assertions.assertEquals("from PROPERTIES", props.get("comment"));
    }

    @Test
    public void deliberatelyEmptyCommentPropertyIsNotOverwritten() {
        Map<String, String> props = new HashMap<>();
        props.put("comment", "");
        IcebergConnectorMetadata.applyCreateTableComment(props, "from the COMMENT clause");
        // WHY: an explicitly-empty property is a user statement of intent, not an absent value -- keyed on
        // containsKey rather than on emptiness so it is preserved.
        Assertions.assertEquals("", props.get("comment"));
    }

    @Test
    public void omittedCommentClauseAddsNoProperty() {
        Map<String, String> props = new HashMap<>();
        // WHY: nereids defaults CreateTableInfo.comment to "" when the clause is omitted, so writing it
        // unconditionally would stamp a noise `"comment" = ""` onto the PROPERTIES of every iceberg table.
        IcebergConnectorMetadata.applyCreateTableComment(props, "");
        Assertions.assertFalse(props.containsKey("comment"));

        IcebergConnectorMetadata.applyCreateTableComment(props, null);
        Assertions.assertFalse(props.containsKey("comment"));
    }
}

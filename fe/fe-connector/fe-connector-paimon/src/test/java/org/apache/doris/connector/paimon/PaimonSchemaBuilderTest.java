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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * P5-T12 — pins {@link PaimonSchemaBuilder#build} to byte-parity with the legacy fe-core
 * {@code PaimonMetadataOps.toPaimonSchema}: this is the function that turns a CREATE TABLE
 * request into the Paimon {@link Schema} actually persisted, so option/key/comment drift here
 * silently changes how new tables are created.
 */
public class PaimonSchemaBuilderTest {

    private static ConnectorColumn col(String name, ConnectorType type, boolean nullable) {
        return new ConnectorColumn(name, type, name + " comment", nullable, null);
    }

    private static ConnectorCreateTableRequest.Builder baseRequest() {
        return ConnectorCreateTableRequest.builder()
                .dbName("db")
                .tableName("t")
                .columns(Arrays.asList(
                        col("id", ConnectorType.of("INT"), false),
                        col("name", ConnectorType.of("STRING"), true)));
    }

    @Test
    public void columnsCarryTypeNameNullabilityAndComment() {
        Schema schema = PaimonSchemaBuilder.build(baseRequest().build());

        // WHY: column name/type/comment and per-column nullability must survive the conversion;
        // nullability is applied via copy(nullable), mirroring legacy toPaimontype().copy(...).
        // MUTATION: dropping .copy(col.isNullable()) (so both columns share paimon's default
        // nullable) or losing the comment turns this red.
        DataField id = schema.fields().get(0);
        DataField name = schema.fields().get(1);
        Assertions.assertEquals("id", id.name());
        Assertions.assertEquals(new IntType(false), id.type(), "non-null column must be copied non-null");
        Assertions.assertEquals("id comment", id.description());
        Assertions.assertEquals("name", name.name());
        Assertions.assertEquals(new VarCharType(VarCharType.MAX_LENGTH).copy(true), name.type(),
                "nullable column must keep nullable, STRING -> VarChar(MAX)");
    }

    @Test
    public void primaryKeysComeFromPropertiesOnly() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("primary-key", "id, name");
        Schema schema = PaimonSchemaBuilder.build(baseRequest().properties(props).build());

        // WHY: primary keys live ONLY in properties["primary-key"], comma-split and trimmed (note
        // the space after the comma above). MUTATION: not trimming (" name") or not reading the
        // property at all (empty pk list) turns this red.
        Assertions.assertEquals(Arrays.asList("id", "name"), schema.primaryKeys());
    }

    @Test
    public void noPrimaryKeyPropertyYieldsEmpty() {
        Schema schema = PaimonSchemaBuilder.build(baseRequest().build());
        // WHY: absent primary-key property must yield an empty pk list, not a NPE or a stray key.
        // MUTATION: defaulting to a non-empty list turns this red.
        Assertions.assertTrue(schema.primaryKeys().isEmpty());
    }

    @Test
    public void identityPartitionSpecBecomesPartitionKeys() {
        ConnectorPartitionSpec spec = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.IDENTITY,
                Arrays.asList(
                        new ConnectorPartitionField("name", "identity", Collections.emptyList()),
                        new ConnectorPartitionField("id", "IDENTITY", Collections.emptyList())),
                Collections.emptyList());
        Schema schema = PaimonSchemaBuilder.build(baseRequest().partitionSpec(spec).build());

        // WHY: identity partition fields map to partition keys by column name, in order, and the
        // identity check is case-insensitive. MUTATION: reordering, or rejecting the upper-case
        // "IDENTITY", turns this red.
        Assertions.assertEquals(Arrays.asList("name", "id"), schema.partitionKeys());
    }

    @Test
    public void primaryKeysResolvedToCanonicalColumnCase() {
        // #65094: columns keep their original case ("Id"); a primary key referenced with a different
        // case ("id") must resolve back to the canonical column name, else Paimon's case-sensitive
        // Schema.Builder validation rejects the table. MUTATION: dropping resolveColumnNames -> the pk
        // stays "id" while the only column is "Id" -> Schema.build() throws -> red.
        ConnectorCreateTableRequest.Builder req = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Arrays.asList(
                        col("Id", ConnectorType.of("INT"), false),
                        col("name", ConnectorType.of("STRING"), true)));
        Map<String, String> props = new LinkedHashMap<>();
        props.put("primary-key", "id");
        Schema schema = PaimonSchemaBuilder.build(req.properties(props).build());
        Assertions.assertEquals(Collections.singletonList("Id"), schema.primaryKeys());
    }

    @Test
    public void partitionKeysResolvedToCanonicalColumnCase() {
        // #65094: same case-insensitive resolution for partition keys ("pt" -> canonical "Pt").
        // MUTATION: dropping resolveColumnNames -> partition key "pt" not among columns {id,Pt} ->
        // Schema.build() throws -> red.
        ConnectorCreateTableRequest.Builder req = ConnectorCreateTableRequest.builder()
                .dbName("db").tableName("t")
                .columns(Arrays.asList(
                        col("id", ConnectorType.of("INT"), false),
                        col("Pt", ConnectorType.of("STRING"), false)));
        ConnectorPartitionSpec spec = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.IDENTITY,
                Collections.singletonList(new ConnectorPartitionField("pt", "identity", Collections.emptyList())),
                Collections.emptyList());
        Schema schema = PaimonSchemaBuilder.build(req.partitionSpec(spec).build());
        Assertions.assertEquals(Collections.singletonList("Pt"), schema.partitionKeys());
    }

    @Test
    public void nullPartitionSpecYieldsNoPartitionKeys() {
        Schema schema = PaimonSchemaBuilder.build(baseRequest().build());
        // WHY: a non-partitioned table (null spec) must yield no partition keys. MUTATION: NPE on
        // null spec, or inventing partition keys, turns this red.
        Assertions.assertTrue(schema.partitionKeys().isEmpty());
    }

    @Test
    public void nonIdentityPartitionTransformThrows() {
        ConnectorPartitionSpec spec = new ConnectorPartitionSpec(
                ConnectorPartitionSpec.Style.TRANSFORM,
                Collections.singletonList(
                        new ConnectorPartitionField("id", "bucket", Collections.singletonList(16))),
                Collections.emptyList());
        // WHY: Paimon legacy only supported plain partition columns; a transform (bucket/year/...)
        // must fail-fast rather than be silently dropped (which would create a differently
        // partitioned table than the user asked for). MUTATION: ignoring the transform and adding
        // the column anyway makes this green-when-it-should-throw -> caught here.
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonSchemaBuilder.build(baseRequest().partitionSpec(spec).build()));
    }

    @Test
    public void locationRekeyedToCorePathAndStripped() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("location", "s3://bucket/path");
        props.put("bucket", "4");
        Schema schema = PaimonSchemaBuilder.build(baseRequest().properties(props).build());

        // WHY: "location" must be removed and re-added under CoreOptions.PATH; unrelated options
        // (bucket) ride through unchanged as passthrough (legacy did not consume bucketSpec).
        // MUTATION: leaving "location" in options, not setting CoreOptions.PATH, or dropping the
        // bucket passthrough turns this red.
        Assertions.assertFalse(schema.options().containsKey("location"),
                "raw location key must be stripped from options");
        Assertions.assertEquals("s3://bucket/path", schema.options().get(CoreOptions.PATH.key()),
                "location must be re-keyed to CoreOptions.PATH");
        Assertions.assertEquals("4", schema.options().get("bucket"),
                "unrelated options must ride through as passthrough");
    }

    @Test
    public void primaryKeyAndCommentStrippedFromOptions() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("primary-key", "id");
        props.put("comment", "from properties");
        props.put("custom", "keep");
        Schema schema = PaimonSchemaBuilder.build(baseRequest().properties(props).build());

        // WHY: "primary-key" and "comment" are control keys consumed into dedicated Schema fields
        // and MUST NOT leak into the option map; other keys remain. MUTATION: leaving either key in
        // options turns this red.
        Assertions.assertFalse(schema.options().containsKey("primary-key"));
        Assertions.assertFalse(schema.options().containsKey("comment"));
        Assertions.assertEquals("keep", schema.options().get("custom"));
    }

    @Test
    public void commentPrefersPropertiesOverClause() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("comment", "from properties");
        Schema schema = PaimonSchemaBuilder.build(
                baseRequest().comment("from clause").properties(props).build());

        // WHY: legacy toPaimonSchema read the table comment ONLY from properties["comment"]; to
        // preserve that persisted-comment behavior, properties wins over the dedicated COMMENT
        // clause. MUTATION: preferring request.getComment() (the clause) flips this to "from
        // clause" -> red.
        Assertions.assertEquals("from properties", schema.comment());
    }

    @Test
    public void commentFallsBackToClauseWhenPropertyAbsent() {
        Schema schema = PaimonSchemaBuilder.build(baseRequest().comment("from clause").build());

        // WHY: when properties has no "comment", the user's COMMENT clause must not be silently
        // dropped (a strictly-legacy reading would lose it). MUTATION: hardcoding null when the
        // property is absent (ignoring request.getComment()) turns this red.
        Assertions.assertEquals("from clause", schema.comment());
    }
}

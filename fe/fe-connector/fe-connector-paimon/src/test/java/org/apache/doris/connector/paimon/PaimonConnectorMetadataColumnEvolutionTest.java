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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonConnectorMetadataColumnEvolutionTest {

    private RecordingPaimonCatalogOps ops;
    private RecordingConnectorContext context;
    private PaimonConnectorMetadata metadata;
    private PaimonTableHandle handle;

    @BeforeEach
    public void setUp() {
        List<DataField> fields = Arrays.asList(
                new DataField(0, "id", new IntType(false), "identifier"),
                new DataField(1, "name", new VarCharType(), null),
                new DataField(2, "score", new IntType(), "initial score", "1"));
        FakePaimonTable table = new FakePaimonTable(
                "t", new RowType(fields), Collections.emptyList(), Collections.singletonList("id"));
        ops = new RecordingPaimonCatalogOps();
        ops.table = table;
        ops.latestSchema = Optional.of(new PaimonCatalogOps.PaimonSchemaSnapshot(
                fields, Collections.emptyList(), Collections.singletonList("id")));
        context = new RecordingConnectorContext();
        metadata = new PaimonConnectorMetadata(
                ops, PaimonCatalogProperties.of(Collections.emptyMap()), context);
        handle = new PaimonTableHandle(
                "db", "t", Collections.emptyList(), Collections.singletonList("id"));
        handle.setPaimonTable(table);
    }

    @Test
    public void addColumnCarriesPositionAndDefaultInOneAlter() {
        ConnectorColumn column = new ConnectorColumn(
                "added", ConnectorType.of("STRING"), "added column", true, "unknown");

        metadata.addColumn(null, handle, column, ConnectorColumnPosition.after("NAME"));

        Assertions.assertEquals(2, context.authCount);
        Assertions.assertEquals("db.t", ops.lastAlteredTableId.getFullName());
        Assertions.assertEquals(2, ops.lastSchemaChanges.size());
        SchemaChange.AddColumn add = (SchemaChange.AddColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"added"}, add.fieldNames());
        Assertions.assertEquals("name", add.move().referenceFieldName());
        SchemaChange.UpdateColumnDefaultValue defaultChange =
                (SchemaChange.UpdateColumnDefaultValue) ops.lastSchemaChanges.get(1);
        Assertions.assertEquals("unknown", defaultChange.newDefaultValue());
    }

    @Test
    public void addColumnsSupportsNarrowIntegerTypes() {
        metadata.addColumns(null, handle, Arrays.asList(
                column("tiny_col", "TINYINT"), column("small_col", "SMALLINT")));

        SchemaChange.AddColumn tiny = (SchemaChange.AddColumn) ops.lastSchemaChanges.get(0);
        SchemaChange.AddColumn small = (SchemaChange.AddColumn) ops.lastSchemaChanges.get(1);
        Assertions.assertInstanceOf(TinyIntType.class, tiny.dataType());
        Assertions.assertInstanceOf(SmallIntType.class, small.dataType());
        Assertions.assertEquals(2, ops.lastSchemaChanges.size());
    }

    @Test
    public void dropAndRenameResolveRemoteNamesCaseInsensitively() {
        metadata.dropColumn(null, handle, "NAME");
        SchemaChange.DropColumn drop = (SchemaChange.DropColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"name"}, drop.fieldNames());

        metadata.renameColumn(null, handle, "NAME", "display_name");
        SchemaChange.RenameColumn rename = (SchemaChange.RenameColumn) ops.lastSchemaChanges.get(0);
        Assertions.assertArrayEquals(new String[] {"name"}, rename.fieldNames());
        Assertions.assertEquals("display_name", rename.newName());
    }

    @Test
    public void modifyColumnCarriesTypeCommentDefaultAndPosition() {
        ConnectorColumn score = new ConnectorColumn(
                "SCORE", ConnectorType.of("BIGINT"), "updated score", true, "10");

        metadata.modifyColumn(null, handle, score, ConnectorColumnPosition.FIRST);

        Assertions.assertEquals(4, ops.lastSchemaChanges.size());
        SchemaChange.UpdateColumnType type = (SchemaChange.UpdateColumnType) ops.lastSchemaChanges.get(0);
        Assertions.assertInstanceOf(BigIntType.class, type.newDataType());
        Assertions.assertInstanceOf(
                SchemaChange.UpdateColumnComment.class, ops.lastSchemaChanges.get(1));
        Assertions.assertInstanceOf(
                SchemaChange.UpdateColumnDefaultValue.class, ops.lastSchemaChanges.get(2));
        Assertions.assertInstanceOf(
                SchemaChange.UpdateColumnPosition.class, ops.lastSchemaChanges.get(3));
    }

    @Test
    public void reorderColumnsEmitsOrderedMoves() {
        metadata.reorderColumns(null, handle, Arrays.asList("name", "ID", "score"));

        Assertions.assertEquals(3, ops.lastSchemaChanges.size());
        SchemaChange.UpdateColumnPosition first =
                (SchemaChange.UpdateColumnPosition) ops.lastSchemaChanges.get(0);
        Assertions.assertEquals("name", first.move().fieldName());
        Assertions.assertEquals("id",
                ((SchemaChange.UpdateColumnPosition) ops.lastSchemaChanges.get(1)).move().fieldName());
    }

    private static ConnectorColumn column(String name, String type) {
        return new ConnectorColumn(name, ConnectorType.of(type), null, true, null);
    }
}

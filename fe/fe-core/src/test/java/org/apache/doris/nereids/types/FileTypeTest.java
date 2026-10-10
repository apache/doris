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

package org.apache.doris.nereids.types;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnToProtobuf;
import org.apache.doris.catalog.ColumnToThrift;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.GsonUtilsCatalog;
import org.apache.doris.thrift.TTypeDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

public class FileTypeTest {
    private DataType fileType() {
        return Assertions.assertDoesNotThrow(() -> DataType.convertFromString("FILE"));
    }

    @Test
    public void testIndependentIdentityAndCanonicalChildren() {
        DataType file = fileType();
        Type catalog = file.toCatalogDataType();
        Assertions.assertEquals(DataType.class, file.getClass().getSuperclass());
        Assertions.assertEquals(Type.class, catalog.getClass().getSuperclass());
        Assertions.assertFalse(file.isStructType());
        Assertions.assertFalse(catalog.isStructType());
        Assertions.assertTrue(file.isComplexType());
        Assertions.assertTrue(catalog.isComplexType());
        Assertions.assertEquals("FILE", file.toSql());
        Assertions.assertEquals("FILE", catalog.toSql());
        Assertions.assertEquals(file, DataType.fromCatalogType(catalog));
        Assertions.assertEquals(catalog, Type.fromPrimitiveType(PrimitiveType.valueOf("FILE")));
        Column column = new Column("f", catalog, true);
        Assertions.assertEquals(Arrays.asList("uri", "offset", "size", "content_type", "checksum", "inline"),
                column.getChildren().stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("varchar(65533)", "bigint", "bigint", "varchar(1024)",
                        "varchar(1024)"), column.getChildren().subList(0, 5).stream()
                .map(child -> child.getType().toSql()).collect(Collectors.toList()));
        Assertions.assertEquals("VARBINARY", column.getChildren().get(5).getDataType().name());
        Assertions.assertTrue(column.getChildren().stream().allMatch(Column::isAllowNull));
        StructType publicStruct = FileType.INSTANCE.publicStructType();
        Assertions.assertEquals(FileType.INSTANCE.getFields(), publicStruct.getFields());
        Assertions.assertEquals(6, publicStruct.getFields().size());
        Assertions.assertEquals(VarBinaryType.INSTANCE, publicStruct.getField("inline").getDataType());
        Assertions.assertTrue(publicStruct.getFields().stream().allMatch(StructField::isNullable));
        Assertions.assertEquals(catalog, new Column("f", PrimitiveType.FILE).getType());
        Assertions.assertFalse(file.acceptsType(StructType.SYSTEM_DEFAULT));
        Assertions.assertFalse(catalog.matchesType(new org.apache.doris.catalog.StructType(
                new java.util.ArrayList<>(Type.FILE.getFields()))));
    }

    @Test
    public void testNestedParsingAndNoParameters() {
        fileType();
        for (String sql : Arrays.asList("ARRAY<FILE>", "MAP<STRING,FILE>", "STRUCT<f:ARRAY<FILE>>")) {
            DataType type = DataType.convertFromString(sql);
            Assertions.assertDoesNotThrow(type::validateDataType);
            Assertions.assertEquals(type, DataType.fromCatalogType(type.toCatalogDataType()));
        }
        Assertions.assertThrows(Exception.class, () -> DataType.convertFromString("FILE(10)"));
        Assertions.assertThrows(Exception.class, () -> DataType.convertFromString("FILE<INT>"));
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle("SELECT file FROM files"));
    }

    @Test
    public void testCanonicalThriftRoundTrip() {
        Type type = fileType().toCatalogDataType();
        TTypeDesc wire = type.toThrift();
        Assertions.assertEquals("FILE", wire.getTypes().get(0).getType().name());
        Assertions.assertEquals(7, wire.getTypesSize());
        Assertions.assertFalse(wire.getTypes().get(0).isSetContainsNulls());
        Assertions.assertTrue(wire.getTypes().get(0).getStructFields().stream()
                .allMatch(field -> field.isSetContainsNull() && field.isContainsNull()));
        Assertions.assertEquals(type, Type.fromThrift(wire));
        TTypeDesc badName = wire.deepCopy();
        badName.getTypes().get(0).getStructFields().get(0).setName("path");
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(badName));
        TTypeDesc badNullable = wire.deepCopy();
        badNullable.getTypes().get(0).getStructFields().get(0).setContainsNull(false);
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(badNullable));
        TTypeDesc badType = wire.deepCopy();
        badType.getTypes().get(1).getScalarType().setLen(1024);
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(badType));
        TTypeDesc missingChild = wire.deepCopy();
        missingChild.getTypes().remove(6);
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(missingChild));
        TTypeDesc wrongCase = wire.deepCopy();
        wrongCase.getTypes().get(0).getStructFields().get(0).setName("URI");
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(wrongCase));
        TTypeDesc redundantNulls = wire.deepCopy();
        redundantNulls.getTypes().get(0).setContainsNulls(Collections.nCopies(6, true));
        Assertions.assertThrows(Exception.class, () -> Type.fromThrift(redundantNulls));
    }

    @Test
    public void testCatalogPersistenceAndSchemaSerializers() throws Exception {
        Column column = new Column("f", fileType().toCatalogDataType(), true);
        String json = GsonUtils.GSON.toJson(column);
        Column restored = GsonUtils.GSON.fromJson(json, Column.class);
        Assertions.assertEquals(column.getType(), restored.getType());
        Assertions.assertEquals(6, restored.getChildren().size());
        Assertions.assertEquals(6, ColumnToThrift.toThrift(restored).getChildrenColumnSize());
        for (int i = 0; i < 6; i++) {
            Assertions.assertEquals(i, restored.getChildren().get(i).getUniqueId());
            Assertions.assertEquals(i, ColumnToThrift.toThrift(restored).getChildrenColumn().get(i).getColUniqueId());
            Assertions.assertEquals(i, ColumnToProtobuf.toPb(restored, Collections.emptySet(),
                    Collections.emptyList()).getChildrenColumns(i).getUniqueId());
        }
        Assertions.assertEquals("VARBINARY", ColumnToThrift.toThrift(restored)
                .getChildrenColumn().get(5).getColumnType().getType().name());
        Assertions.assertEquals("STRING", ColumnToProtobuf.toPb(restored, Collections.emptySet(),
                Collections.emptyList()).getChildrenColumns(5).getType());
        Assertions.assertEquals("FILE", ColumnToProtobuf.toPb(restored, Collections.emptySet(),
                Collections.emptyList()).getType());
        Assertions.assertEquals(6, ColumnToProtobuf.toPb(restored, Collections.emptySet(),
                Collections.emptyList()).getChildrenColumnsCount());
        Assertions.assertThrows(Exception.class,
                () -> GsonUtils.GSON.fromJson(json.replace("\"uri\"", "\"path\""), Column.class));
        Column catalogRestored = GsonUtilsCatalog.GSON.fromJson(GsonUtilsCatalog.GSON.toJson(column), Column.class);
        Assertions.assertEquals("FILE", catalogRestored.getType().toSql());
        Assertions.assertEquals(6, catalogRestored.getChildren().size());
    }

    @Test
    public void testOldExecutionVersionRejectsExecutionButAllowsReplay() {
        Type type = fileType().toCatalogDataType();
        TTypeDesc wire = type.toThrift();
        String json = GsonUtils.GSON.toJson(type, Type.class);
        int oldVersion = Config.be_exec_version;
        try {
            Config.be_exec_version = 14;
            Assertions.assertEquals(type, Type.fromThrift(wire));
            Type restored = GsonUtils.GSON.fromJson(json, Type.class);
            Assertions.assertEquals("FILE", restored.toSql());
            Assertions.assertEquals(type, restored);
            Assertions.assertThrows(IllegalStateException.class, type::toThrift);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> new org.apache.doris.catalog.ArrayType(type).toThrift());
        } finally {
            Config.be_exec_version = oldVersion;
        }
    }

    @Test
    public void testMysqlMetadata() throws Exception {
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING, PrimitiveType.FILE.toMysqlType());
        Assertions.assertTrue(Type.FILE.getColumnSize() >= 6 * (65533 + 1024 + 1024) + 100);
        Assertions.assertEquals(Type.FILE.getColumnSize().intValue(), Type.FILE.getColumnStringRepSize());
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeField("f", Type.FILE);
        ByteBuffer packet = serializer.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < 6; i++) {
            int length = packet.get() & 0xff;
            packet.position(packet.position() + length);
        }
        Assertions.assertEquals(12, packet.get() & 0xff);
        Assertions.assertEquals(33, packet.getShort());
        Assertions.assertEquals(Type.FILE.getColumnSize().intValue(), packet.getInt());
        Assertions.assertEquals(MysqlColType.MYSQL_TYPE_STRING.getCode(), packet.get() & 0xff);
        Assertions.assertEquals(0, packet.getShort());
    }

    @Test
    public void testLogicalNestingDepthMatchesScalarLeaf() {
        Type file = fileType().toCatalogDataType();
        Type scalar = Type.BIGINT;
        for (int depth = 0; depth <= Type.MAX_NESTING_DEPTH + 1; depth++) {
            Assertions.assertEquals(scalar.exceedsMaxNestingDepth(), file.exceedsMaxNestingDepth());
            file = new org.apache.doris.catalog.ArrayType(file);
            scalar = new org.apache.doris.catalog.ArrayType(scalar);
        }
    }

    @Test
    public void testContainsFileAcrossNestedTypes() {
        for (String sql : Arrays.asList("FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>")) {
            Type type = DataType.convertFromString(sql).toCatalogDataType();
            Assertions.assertTrue(type.typeContainsFile());
        }
        for (String sql : Arrays.asList("BIGINT", "ARRAY<INT>", "STRUCT<f:STRING>", "MAP<STRING,INT>")) {
            Assertions.assertFalse(DataType.convertFromString(sql).toCatalogDataType().typeContainsFile());
        }
    }

    @Test
    public void testRejectMalformedNestedColumnMetadata() {
        Column nested = new Column("a", new org.apache.doris.catalog.ArrayType(fileType().toCatalogDataType()));
        nested.getChildren().get(0).getChildren().remove(5);
        Assertions.assertThrows(Exception.class, () -> ColumnToThrift.toThrift(nested));
        Assertions.assertThrows(Exception.class, () -> ColumnToProtobuf.toPb(nested,
                Collections.emptySet(), Collections.emptyList()));
        Assertions.assertThrows(Exception.class, () -> GsonUtils.GSON.toJson(nested));
        Assertions.assertThrows(Exception.class,
                () -> GsonUtils.GSON.fromJson("{\"clazz\":\"FileType\"}", Type.class));
        Assertions.assertThrows(Exception.class, () -> GsonUtils.GSON.fromJson(
                "{\"clazz\":\"ScalarType\",\"type\":\"FILE\"}", Type.class));
    }
}

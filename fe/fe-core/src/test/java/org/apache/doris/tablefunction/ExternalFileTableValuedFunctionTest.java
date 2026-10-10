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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStructField;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExternalFileTableValuedFunctionTest {
    private static List<PTypeNode> fileNodes() {
        PTypeNode.Builder file = PTypeNode.newBuilder().setType(TTypeNodeType.FILE.getValue());
        List<PTypeNode> children = new ArrayList<>();
        for (StructField field : FileType.create().getFields()) {
            file.addStructFields(PStructField.newBuilder().setName(field.getName()).setContainsNull(true));
            children.add(PTypeNode.newBuilder().setType(TTypeNodeType.SCALAR.getValue())
                    .setScalarType(PScalarType.newBuilder()
                            .setType(field.getType().getPrimitiveType().toThrift().getValue())
                            .setLen(field.getType().isVarbinaryType() ? -1 : field.getType().getLength())).build());
        }
        children.add(0, file.build());
        return children;
    }

    @SuppressWarnings("unchecked")
    private static Pair<Type, Integer> parseNodes(List<PTypeNode> nodes) throws Exception {
        ExternalFileTableValuedFunction tvf = Mockito.mock(
                ExternalFileTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        Method method = ExternalFileTableValuedFunction.class
                .getDeclaredMethod("getColumnType", List.class, int.class);
        method.setAccessible(true);
        return (Pair<Type, Integer>) method.invoke(tvf, nodes, 0);
    }

    @Test
    public void testFileSchemaIdentityAndRecursiveNodeConsumption() throws Exception {
        Pair<Type, Integer> file = parseNodes(fileNodes());
        Assertions.assertEquals(FileType.create(), file.key());
        Assertions.assertEquals(7, file.value());
        List<PTypeNode> bounded = fileNodes();
        bounded.set(6, bounded.get(6).toBuilder().setScalarType(bounded.get(6).getScalarType().toBuilder()
                .setLen(ScalarType.MAX_VARBINARY_LENGTH)).build());
        Assertions.assertEquals(FileType.create(), parseNodes(bounded).key());
        List<PTypeNode> array = new ArrayList<>();
        array.add(PTypeNode.newBuilder().setType(TTypeNodeType.ARRAY.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.ARRAY.getValue())).build());
        array.addAll(fileNodes());
        Assertions.assertEquals(FileType.create(), ((ArrayType) parseNodes(array).key()).getItemType());

        List<PTypeNode> map = new ArrayList<>();
        map.add(PTypeNode.newBuilder().setType(TTypeNodeType.MAP.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.MAP.getValue())).build());
        map.add(PTypeNode.newBuilder().setType(TTypeNodeType.SCALAR.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.STRING.getValue())).build());
        map.addAll(fileNodes());
        Assertions.assertEquals(FileType.create(), ((MapType) parseNodes(map).key()).getValueType());

        List<PTypeNode> nested = new ArrayList<>();
        PTypeNode.Builder structure = PTypeNode.newBuilder().setType(TTypeNodeType.STRUCT.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.STRUCT.getValue()));
        for (String name : Arrays.asList("asset", "attachments", "lookup", "tail")) {
            structure.addStructFields(PStructField.newBuilder().setName(name).setContainsNull(true));
        }
        nested.add(structure.build());
        nested.addAll(fileNodes());
        nested.addAll(array);
        nested.addAll(map);
        nested.add(PTypeNode.newBuilder().setType(TTypeNodeType.SCALAR.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.BIGINT.getValue())).build());
        Pair<Type, Integer> parsed = parseNodes(nested);
        Assertions.assertEquals(nested.size(), parsed.value());
        List<StructField> fields = ((StructType) parsed.key()).getFields();
        Assertions.assertEquals(FileType.create(), fields.get(0).getType());
        Assertions.assertEquals(FileType.create(), ((ArrayType) fields.get(1).getType()).getItemType());
        Assertions.assertEquals(FileType.create(), ((MapType) fields.get(2).getType()).getValueType());
        Assertions.assertEquals(Type.BIGINT, fields.get(3).getType());
    }

    @Test
    public void testFileSchemaRejectsNoncanonicalWireChildren() {
        for (int invalid : Arrays.asList(0, 1, 2, 3, 4)) {
            List<PTypeNode> nodes = fileNodes();
            PTypeNode.Builder file = nodes.get(0).toBuilder();
            if (invalid == 0) {
                file.removeStructFields(5);
            } else if (invalid == 1) {
                file.setStructFields(0, file.getStructFields(0).toBuilder().setName("URI"));
            } else if (invalid == 2) {
                file.setStructFields(0, file.getStructFields(0).toBuilder().setContainsNull(false));
            } else if (invalid == 3) {
                nodes.set(6, nodes.get(6).toBuilder().setScalarType(
                        PScalarType.newBuilder().setType(TPrimitiveType.STRING.getValue())).build());
            } else {
                nodes.set(6, nodes.get(6).toBuilder().setScalarType(nodes.get(6).getScalarType().toBuilder()
                        .setLen(32)).build());
            }
            nodes.set(0, file.build());
            InvocationTargetException error = Assertions.assertThrows(InvocationTargetException.class,
                    () -> parseNodes(nodes));
            Assertions.assertInstanceOf(IllegalArgumentException.class, error.getCause());
        }
    }

    @Test
    public void testFileSchemaPreservesNestedFieldSpelling() throws Exception {
        ExternalFileTableValuedFunction tvf = Mockito.mock(
                ExternalFileTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        PTypeNode structNode = PTypeNode.newBuilder()
                .setType(TTypeNodeType.STRUCT.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.STRUCT.getValue()))
                .addStructFields(PStructField.newBuilder()
                        .setName("CaseSensitive")
                        .setComment("mixed-case child")
                        .setContainsNull(true))
                .build();
        PTypeNode intNode = PTypeNode.newBuilder()
                .setType(TTypeNodeType.SCALAR.getValue())
                .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.INT.getValue()))
                .build();

        Method getColumnType = ExternalFileTableValuedFunction.class
                .getDeclaredMethod("getColumnType", List.class, int.class);
        getColumnType.setAccessible(true);
        @SuppressWarnings("unchecked")
        Pair<Type, Integer> parsed = (Pair<Type, Integer>) getColumnType.invoke(
                tvf, Arrays.asList(structNode, intNode), 0);

        StructField field = ((StructType) parsed.key()).getFields().get(0);
        Assertions.assertEquals("casesensitive", field.getName());
        Assertions.assertEquals("CaseSensitive", field.getOriginalName());
        Assertions.assertEquals("mixed-case child", field.getComment());
        Assertions.assertTrue(field.getContainsNull());
    }

    @Test
    public void testHiveParquetTimeZoneIsCanonicalizedAndRemovedFromStorageProperties()
            throws AnalysisException {
        ExternalFileTableValuedFunction tvf = Mockito.mock(
                ExternalFileTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_PARQUET);
        properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, "8:00");

        Map<String, String> storageProperties = tvf.parseCommonProperties(properties);

        Assertions.assertEquals("+08:00", tvf.getHiveParquetTimeZone());
        Assertions.assertFalse(storageProperties.containsKey(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE));
    }

    @Test
    public void testHiveParquetTimeZoneRejectsAmbiguousShortAlias() {
        ExternalFileTableValuedFunction tvf = Mockito.mock(
                ExternalFileTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_PARQUET);
        properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, "CST");

        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> tvf.parseCommonProperties(properties));

        Assertions.assertTrue(exception.getMessage().contains("short timezone aliases are not supported"));
    }

    @Test
    public void testCsvSchemaUuid() throws Exception {
        List<Column> columns = Lists.newArrayList();
        FileFormatUtils.parseCsvSchema(columns, "id:int;u: UUID ;v:uuid");
        Assertions.assertEquals(3, columns.size());
        Assertions.assertEquals(Type.UUID, columns.get(1).getType());
        Assertions.assertEquals(Type.UUID, columns.get(2).getType());
        Assertions.assertTrue(columns.get(1).isAllowNull());
        Assertions.assertEquals("u", columns.get(1).getName());

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> FileFormatUtils.parseCsvSchema(Lists.newArrayList(), "u:uuid(16)"));
        Assertions.assertTrue(exception.getMessage().contains("unsupported column type: uuid(16)"));
    }

    @Test
    public void testCsvSchemaParse() {
        Config.enable_date_conversion = true;
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:bool;"
                        + "k8:char(10);k9:varchar(20);k10:date;k11:datetime;k12:decimal(10,2)");
        List<Column> csvSchema = Lists.newArrayList();
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assertions.fail();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assertions.assertTrue(e.getMessage().contains("unsupported column type: bool"));
        }

        csvSchema.clear();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:boolean;"
                        + "k8:string;k9:date;k10:datetime;k11:decimal(10, 2);k12:decimal( 38,10); k13:datetime(5)");
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assertions.assertEquals(13, csvSchema.size());
            Column decimalCol = csvSchema.get(10);
            Assertions.assertEquals(10, decimalCol.getPrecision());
            Assertions.assertEquals(2, decimalCol.getScale());
            decimalCol = csvSchema.get(11);
            Assertions.assertEquals(38, decimalCol.getPrecision());
            Assertions.assertEquals(10, decimalCol.getScale());
            Column datetimeCol = csvSchema.get(12);
            Assertions.assertEquals(5, datetimeCol.getScale());

            for (int i = 0; i < csvSchema.size(); i++) {
                Column col = csvSchema.get(i);
                switch (col.getName()) {
                    case "k1":
                        Assertions.assertEquals(PrimitiveType.INT, col.getType().getPrimitiveType());
                        break;
                    case "k2":
                        Assertions.assertEquals(PrimitiveType.BIGINT, col.getType().getPrimitiveType());
                        break;
                    case "k3":
                        Assertions.assertEquals(PrimitiveType.FLOAT, col.getType().getPrimitiveType());
                        break;
                    case "k4":
                        Assertions.assertEquals(PrimitiveType.DOUBLE, col.getType().getPrimitiveType());
                        break;
                    case "k5":
                        Assertions.assertEquals(PrimitiveType.SMALLINT, col.getType().getPrimitiveType());
                        break;
                    case "k6":
                        Assertions.assertEquals(PrimitiveType.TINYINT, col.getType().getPrimitiveType());
                        break;
                    case "k7":
                        Assertions.assertEquals(PrimitiveType.BOOLEAN, col.getType().getPrimitiveType());
                        break;
                    case "k8":
                        Assertions.assertEquals(PrimitiveType.STRING, col.getType().getPrimitiveType());
                        break;
                    case "k9":
                        Assertions.assertEquals(PrimitiveType.DATEV2, col.getType().getPrimitiveType());
                        break;
                    case "k10":
                        Assertions.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    case "k11":
                        Assertions.assertEquals(PrimitiveType.DECIMAL64, col.getType().getPrimitiveType());
                        break;
                    case "k12":
                        Assertions.assertEquals(PrimitiveType.DECIMAL128, col.getType().getPrimitiveType());
                        break;
                    case "k13":
                        Assertions.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    default:
                        Assertions.fail("unknown column name: " + col.getName());
                }
            }
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}

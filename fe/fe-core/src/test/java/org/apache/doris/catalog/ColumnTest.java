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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TColumn;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ColumnTest {

    private Env env;

    private FakeEnv fakeEnv;

    @BeforeEach
    public void setUp() {
        fakeEnv = new FakeEnv();
        env = Deencapsulation.newInstance(Env.class);

        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @AfterEach
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
    }

    @Test
    public void testSchemaChangeDefaultExpressionSerialization() throws Exception {
        Column column = new Column("ts", Type.TIMESTAMP_NS, false, null, true,
                "CURRENT_TIMESTAMP(9)", "", true, null, 1,
                "2000-01-01 00:00:00.000000000");

        TColumn thriftColumn = ColumnToThrift.toThrift(column);

        Assertions.assertEquals("2000-01-01 00:00:00.000000000", thriftColumn.getDefaultValue());
        Assertions.assertEquals("CURRENT_TIMESTAMP(9)", thriftColumn.getDefaultValueExpr());
        Assertions.assertEquals(ScalarType.TIMESTAMP_NS_PRECISION,
                thriftColumn.getColumnType().getPrecision());
        Assertions.assertEquals(ScalarType.TIMESTAMP_NS_SCALE, thriftColumn.getColumnType().getScale());

        OlapFile.ColumnPB protobufColumn = ColumnToProtobuf.toPb(column, null, null);
        Assertions.assertEquals("2000-01-01 00:00:00.000000000",
                protobufColumn.getDefaultValue().toStringUtf8());
        Assertions.assertEquals("CURRENT_TIMESTAMP(9)", protobufColumn.getDefaultValueExpr().toStringUtf8());
        Assertions.assertEquals(ScalarType.TIMESTAMP_NS_PRECISION, protobufColumn.getPrecision());
        Assertions.assertEquals(ScalarType.TIMESTAMP_NS_SCALE, protobufColumn.getFrac());

        Column datetimeColumn = new Column("dt", ScalarType.createDatetimeV2Type(6), false, null, true,
                "CURRENT_TIMESTAMP(6)", "", true, null, 2,
                "2000-01-01 00:00:00.000000");
        Assertions.assertFalse(ColumnToThrift.toThrift(datetimeColumn).isSetDefaultValueExpr());
        OlapFile.ColumnPB datetimeProtobufColumn = ColumnToProtobuf.toPb(datetimeColumn, null, null);
        Assertions.assertEquals("CURRENT_TIMESTAMP(6)", datetimeProtobufColumn.getDefaultValue().toStringUtf8());
        Assertions.assertFalse(datetimeProtobufColumn.hasDefaultValueExpr());
    }


    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createTempFile("columnTest", "tmp");
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        Column column1 = new Column("user",
                                ScalarType.createChar(20), false, AggregateType.SUM, "", "");
        Text.writeString(dos, GsonUtils.GSON.toJson(column1));
        Column column2 = new Column("age",
                                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20", "");
        Text.writeString(dos, GsonUtils.GSON.toJson(column2));

        Column column3 = new Column("name", PrimitiveType.BIGINT);
        column3.setIsKey(true);
        Text.writeString(dos, GsonUtils.GSON.toJson(column3));

        Column column4 = new Column("age",
                                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "20",
                                    "");
        Text.writeString(dos, GsonUtils.GSON.toJson(column4));

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        Column rColumn1 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assertions.assertEquals("user", rColumn1.getName());
        Assertions.assertEquals(PrimitiveType.CHAR, rColumn1.getDataType());
        Assertions.assertEquals(AggregateType.SUM, rColumn1.getAggregationType());
        Assertions.assertEquals("", rColumn1.getDefaultValue());
        Assertions.assertEquals(0, rColumn1.getScale());
        Assertions.assertEquals(0, rColumn1.getPrecision());
        Assertions.assertEquals(20, rColumn1.getStrLen());
        Assertions.assertFalse(rColumn1.isAllowNull());

        // 3. Test read()
        Column rColumn2 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assertions.assertEquals("age", rColumn2.getName());
        Assertions.assertEquals(PrimitiveType.INT, rColumn2.getDataType());
        Assertions.assertEquals(AggregateType.REPLACE, rColumn2.getAggregationType());
        Assertions.assertEquals("20", rColumn2.getDefaultValue());

        Column rColumn3 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assertions.assertEquals(rColumn3, column3);

        Column rColumn4 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assertions.assertEquals(rColumn4, column4);

        Assertions.assertEquals(rColumn2.toString(), column2.toString());
        Assertions.assertEquals(column1, column1);

        // 4. delete files
        dis.close();
        Files.delete(path);
    }

    @Test
    public void testSchemaChangeAllowed() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, true, "0", "");
            Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeIntToVarchar() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ScalarType.createType(PrimitiveType.INT), false, null, true, "0", "");
            Column newColumn = new Column("a", ScalarType.createType(PrimitiveType.VARCHAR, 1, 0, 0), false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeFloatToVarchar() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("b", ScalarType.createType(PrimitiveType.FLOAT), false, null, true, "0", "");
            Column newColumn = new Column("b", ScalarType.createType(PrimitiveType.VARCHAR, 23, 0, 0), false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeDecimalToVarchar() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ScalarType.createType(PrimitiveType.DECIMALV2, 13, 13, 3), false, null, true, "0", "");
            Column newColumn = new Column("a", ScalarType.createType(PrimitiveType.VARCHAR, 14, 0, 0), false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeDoubleToVarchar() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("c", ScalarType.createType(PrimitiveType.DOUBLE), false, null, true, "0", "");
            Column newColumn = new Column("c", ScalarType.createType(PrimitiveType.VARCHAR, 31,  0, 0), false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeArrayToArray() throws DdlException {
        Column oldColumn = new Column("a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        Column newColumn = new Column("a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testStrictNestedPrimitivePromotionRules() {
        Assertions.assertTrue(ColumnType.isSupportedStrictNestedPrimitivePromotion(Type.TINYINT, Type.INT));
        Assertions.assertTrue(ColumnType.isSupportedStrictNestedPrimitivePromotion(Type.FLOAT, Type.DOUBLE));

        Assertions.assertFalse(ColumnType.isSupportedStrictNestedPrimitivePromotion(Type.INT, Type.FLOAT));
        Assertions.assertFalse(ColumnType.isSupportedStrictNestedPrimitivePromotion(Type.VARCHAR, Type.INT));
        Assertions.assertFalse(ColumnType.isSupportedStrictNestedPrimitivePromotion(
                ScalarType.createDecimalV3Type(5, 2), ScalarType.createDecimalV3Type(10, 2)));
        Assertions.assertFalse(ColumnType.isSupportedStrictNestedPrimitivePromotion(
                ScalarType.createDecimalV3Type(10, 2), ScalarType.createDecimalV3Type(5, 2)));
        Assertions.assertFalse(ColumnType.isSupportedStrictNestedPrimitivePromotion(
                ScalarType.createDecimalV3Type(5, 2), ScalarType.createDecimalV3Type(10, 3)));
    }

    @Test
    public void testIcebergNestedDecimalPromotionRules() {
        Assertions.assertTrue(ColumnType.isSupportedIcebergNestedDecimalPromotion(
                ScalarType.createDecimalV3Type(5, 2), ScalarType.createDecimalV3Type(10, 2)));

        Assertions.assertFalse(ColumnType.isSupportedIcebergNestedDecimalPromotion(Type.INT, Type.BIGINT));
        Assertions.assertFalse(ColumnType.isSupportedIcebergNestedDecimalPromotion(
                ScalarType.createDecimalV3Type(10, 2), ScalarType.createDecimalV3Type(5, 2)));
        Assertions.assertFalse(ColumnType.isSupportedIcebergNestedDecimalPromotion(
                ScalarType.createDecimalV3Type(5, 2), ScalarType.createDecimalV3Type(10, 3)));
    }

    @Test
    public void testSchemaChangeArrayDecimalPrecisionPromotionRejectedForInternalTable() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(5, 2), true),
                    false, null, true, "0", "");
            Column newColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(10, 2), true),
                    false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeMapDecimalValuePrecisionPromotionRejectedForInternalTable() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", new MapType(Type.INT, ScalarType.createDecimalV3Type(5, 2)),
                    false, null, true, "0", "");
            Column newColumn = new Column("a", new MapType(Type.INT, ScalarType.createDecimalV3Type(10, 2)),
                    false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeStructDecimalFieldPrecisionPromotionRejectedForInternalTable() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a",
                    new StructType(new StructField("d", ScalarType.createDecimalV3Type(5, 2))),
                    false, null, true, "0", "");
            Column newColumn = new Column("a",
                    new StructType(new StructField("d", ScalarType.createDecimalV3Type(10, 2))),
                    false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeArrayDecimalPrecisionNarrowing() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(10, 2), true),
                    false, null, true, "0", "");
            Column newColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(5, 2), true),
                    false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeArrayDecimalScaleChange() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(5, 2), true),
                    false, null, true, "0", "");
            Column newColumn = new Column("a", ArrayType.create(ScalarType.createDecimalV3Type(10, 3), true),
                    false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testSchemaChangeArrayToArrayDowngrade() throws DdlException {
        Assertions.assertThrows(DdlException.class, () -> {
            Column oldColumn = new Column("a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
            Column newColumn = new Column("a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testBaseColumn() {
        Column baseColumn = new Column("base_a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        SlotDescriptor baseDescriptor = new SlotDescriptor(new SlotId(0), null);
        baseDescriptor.setColumn(baseColumn);
        SlotRef baseSlot = new SlotRef(baseDescriptor);
        Column mvColumnSimple = new Column("mv_a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        mvColumnSimple.setDefineExpr(baseSlot);
        Assertions.assertTrue(mvColumnSimple.tryGetBaseColumnName().equalsIgnoreCase("base_a"));
        Expr add = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, baseSlot, baseSlot, ScalarType.BOOLEAN, NullableMode.DEPEND_ON_ARGUMENT, true);
        Column mvColumnComplex = new Column("mv_b", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        mvColumnComplex.setDefineExpr(add);
        Assertions.assertTrue(mvColumnComplex.tryGetBaseColumnName().equalsIgnoreCase("mv_b"));
    }
}

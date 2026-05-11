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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class ColumnTest {

    private Env env;

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        fakeEnv = new FakeEnv();
        env = Deencapsulation.newInstance(Env.class);

        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @After
    public void tearDown() {
        if (fakeEnv != null) {
            fakeEnv.close();
        }
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
        Assert.assertEquals("user", rColumn1.getName());
        Assert.assertEquals(PrimitiveType.CHAR, rColumn1.getDataType());
        Assert.assertEquals(AggregateType.SUM, rColumn1.getAggregationType());
        Assert.assertEquals("", rColumn1.getDefaultValue());
        Assert.assertEquals(0, rColumn1.getScale());
        Assert.assertEquals(0, rColumn1.getPrecision());
        Assert.assertEquals(20, rColumn1.getStrLen());
        Assert.assertFalse(rColumn1.isAllowNull());

        // 3. Test read()
        Column rColumn2 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assert.assertEquals("age", rColumn2.getName());
        Assert.assertEquals(PrimitiveType.INT, rColumn2.getDataType());
        Assert.assertEquals(AggregateType.REPLACE, rColumn2.getAggregationType());
        Assert.assertEquals("20", rColumn2.getDefaultValue());

        Column rColumn3 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assert.assertEquals(rColumn3, column3);

        Column rColumn4 = GsonUtils.GSON.fromJson(Text.readString(dis), Column.class);
        Assert.assertEquals(rColumn4, column4);

        Assert.assertEquals(rColumn2.toString(), column2.toString());
        Assert.assertEquals(column1, column1);

        // 4. delete files
        dis.close();
        Files.delete(path);
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeAllowed() throws DdlException {
        Column oldColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, true, "0", "");
        Column newColumn = new Column("user", ScalarType.createType(PrimitiveType.INT), true, null, false, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeIntToVarchar() throws DdlException {
        Column oldColumn = new Column("a", ScalarType.createType(PrimitiveType.INT), false, null, true, "0", "");
        Column newColumn = new Column("a", ScalarType.createType(PrimitiveType.VARCHAR, 1, 0, 0), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeFloatToVarchar() throws DdlException {
        Column oldColumn = new Column("b", ScalarType.createType(PrimitiveType.FLOAT), false, null, true, "0", "");
        Column newColumn = new Column("b", ScalarType.createType(PrimitiveType.VARCHAR, 23, 0, 0), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeDecimalToVarchar() throws DdlException {
        Column oldColumn = new Column("a", ScalarType.createType(PrimitiveType.DECIMALV2, 13, 13, 3), false, null, true, "0", "");
        Column newColumn = new Column("a", ScalarType.createType(PrimitiveType.VARCHAR, 14, 0, 0), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeDoubleToVarchar() throws DdlException {
        Column oldColumn = new Column("c", ScalarType.createType(PrimitiveType.DOUBLE), false, null, true, "0", "");
        Column newColumn = new Column("c", ScalarType.createType(PrimitiveType.VARCHAR, 31,  0, 0), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testSchemaChangeArrayToArray() throws DdlException {
        Column oldColumn = new Column("a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        Column newColumn = new Column("a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test(expected = DdlException.class)
    public void testSchemaChangeArrayToArrayDowngrade() throws DdlException {
        Column oldColumn = new Column("a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        Column newColumn = new Column("a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }

    @Test
    public void testVariantSchemaTemplateAppendAllowed() throws DdlException {
        Column oldColumn = variantColumn("v",
                variantField("a", Type.STRING, PatternType.MATCH_NAME));
        Column newColumn = variantColumn("v",
                variantField("a", Type.STRING, PatternType.MATCH_NAME),
                variantField("b", Type.INT, PatternType.MATCH_NAME));
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testVariantDocMaterializationMinRowsChangeAllowed() throws DdlException {
        Column oldColumn = variantColumnWithDocMaterializationMinRows("v", 10);
        Column newColumn = variantColumnWithDocMaterializationMinRows("v", 2);
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testVariantSchemaTemplateAppendWithDocMaterializationMinRowsChangeRejected() {
        assertSchemaChangeFails(
                variantColumnWithDocMaterializationMinRows("v", 10,
                        variantField("a", Type.STRING, PatternType.MATCH_NAME)),
                variantColumnWithDocMaterializationMinRows("v", 2,
                        variantField("a", Type.STRING, PatternType.MATCH_NAME),
                        variantField("b", Type.INT, PatternType.MATCH_NAME)),
                "materialization min rows when changing variant schema templates");
    }

    @Test
    public void testVariantSchemaTemplateIncompatibleChanges() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME),
                        variantField("b", Type.INT, PatternType.MATCH_NAME)),
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME)),
                "reduce variant schema templates");
        assertSchemaChangeFails(
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME),
                        variantField("b", Type.INT, PatternType.MATCH_NAME)),
                variantColumn("v", variantField("b", Type.INT, PatternType.MATCH_NAME),
                        variantField("a", Type.STRING, PatternType.MATCH_NAME)),
                "reorder or rename variant schema templates");
        assertSchemaChangeFails(
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME)),
                variantColumn("v", variantField("a", Type.INT, PatternType.MATCH_NAME)),
                "schema template type");
        assertSchemaChangeFails(
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME)),
                variantColumn("v", variantField("a", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "pattern type");
        assertSchemaChangeFails(
                variantColumn("v", new VariantField("a", Type.STRING, "old", PatternType.MATCH_NAME)),
                variantColumn("v", new VariantField("a", Type.STRING, "new", PatternType.MATCH_NAME)),
                "schema template comment");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedExactPath() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("metric.name", Type.STRING, PatternType.MATCH_NAME)),
                "already matches it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedExactPathInSameAlter() {
        assertSchemaChangeFails(
                variantColumn("v"),
                variantColumn("v", variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("metric.name", Type.STRING, PatternType.MATCH_NAME)),
                "already matches it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedGlobPath() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("content?", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedGlobPathInSameAlter() {
        assertSchemaChangeFails(
                variantColumn("v"),
                variantColumn("v", variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("metric.name*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedQuestionGlobPath() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("metric.?", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("metric.?", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("metric.[0-9]", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedMiddleStarGlobPath() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("a?b", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendShadowedLiteralGlobPath() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("ab", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendDifferentGlobPrefixAllowed() throws DdlException {
        Column oldColumn = variantColumn("v",
                variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB));
        Column newColumn = variantColumn("v",
                variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                variantField("other.*", Type.STRING, PatternType.MATCH_NAME_GLOB));
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testVariantSchemaTemplateAppendOverlappingGlobAllowed() throws DdlException {
        Column oldColumn = variantColumn("v",
                variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB));
        Column newColumn = variantColumn("v",
                variantField("a*b", Type.STRING, PatternType.MATCH_NAME_GLOB),
                variantField("a*", Type.STRING, PatternType.MATCH_NAME_GLOB));
        oldColumn.checkSchemaChangeAllowed(newColumn);
    }

    @Test
    public void testVariantSchemaTemplateAppendComplexGlobContainmentRejected() {
        assertSchemaChangeFails(
                variantColumn("v", variantField("a?*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                variantColumn("v", variantField("a?*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("ab*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "can shadow it");
    }

    @Test
    public void testVariantSchemaTemplateAppendDuplicateExactPathInSameAlter() {
        assertSchemaChangeFails(
                variantColumn("v"),
                variantColumn("v", variantField("metric.name", Type.STRING, PatternType.MATCH_NAME),
                        variantField("metric.name", Type.STRING, PatternType.MATCH_NAME)),
                "duplicate variant schema template");
    }

    @Test
    public void testVariantSchemaTemplateAppendDuplicateGlobPathInSameAlter() {
        assertSchemaChangeFails(
                variantColumn("v"),
                variantColumn("v", variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB),
                        variantField("metric.*", Type.STRING, PatternType.MATCH_NAME_GLOB)),
                "duplicate variant schema template");
    }

    private static Column variantColumn(String name, VariantField... fields) {
        return new Column(name, variantType(fields), false, null, true, null, "");
    }

    private static Column variantColumnWithDocMaterializationMinRows(String name, long materializationMinRows,
            VariantField... fields) {
        return new Column(name, new VariantType(predefinedFields(fields), 0, false, 10000, 0, true,
                materializationMinRows, 64, false), false, null, true, null, "");
    }

    private static VariantType variantType(VariantField... fields) {
        return new VariantType(predefinedFields(fields));
    }

    private static ArrayList<VariantField> predefinedFields(VariantField... fields) {
        ArrayList<VariantField> predefinedFields = new ArrayList<>();
        for (VariantField field : fields) {
            predefinedFields.add(field);
        }
        return predefinedFields;
    }

    private static VariantField variantField(String pattern, Type type, PatternType patternType) {
        return new VariantField(pattern, type, "", patternType);
    }

    private static void assertSchemaChangeFails(Column oldColumn, Column newColumn, String expectedMessage) {
        try {
            oldColumn.checkSchemaChangeAllowed(newColumn);
            Assert.fail("No exception throws.");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void testBaseColumn() {
        Column baseColumn = new Column("base_a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        SlotDescriptor baseDescriptor = new SlotDescriptor(new SlotId(0), null);
        baseDescriptor.setColumn(baseColumn);
        SlotRef baseSlot = new SlotRef(baseDescriptor);
        Column mvColumnSimple = new Column("mv_a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        mvColumnSimple.setDefineExpr(baseSlot);
        Assert.assertTrue(mvColumnSimple.tryGetBaseColumnName().equalsIgnoreCase("base_a"));
        Expr add = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, baseSlot, baseSlot, ScalarType.BOOLEAN, NullableMode.DEPEND_ON_ARGUMENT, true);
        Column mvColumnComplex = new Column("mv_b", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        mvColumnComplex.setDefineExpr(add);
        Assert.assertTrue(mvColumnComplex.tryGetBaseColumnName().equalsIgnoreCase("mv_b"));
    }
}

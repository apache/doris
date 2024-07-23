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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

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

    @Test(expected = DdlException.class)
    public void testSchemaChangeArrayToArray() throws DdlException {
        Column oldColumn = new Column("a", ArrayType.create(Type.TINYINT, true), false, null, true, "0", "");
        Column newColumn = new Column("a", ArrayType.create(Type.INT, true), false, null, true, "0", "");
        oldColumn.checkSchemaChangeAllowed(newColumn);
        Assert.fail("No exception throws.");
    }
}

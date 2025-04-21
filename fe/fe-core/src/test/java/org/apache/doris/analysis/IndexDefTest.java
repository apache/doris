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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class IndexDefTest {
    private IndexDef def;

    @Before
    public void setUp() throws Exception {
        def = new IndexDef("index1", false, Lists.newArrayList("col1"), IndexDef.IndexType.INVERTED, null, "balabala");
    }

    @Test
    public void testAnalyzeNormal() throws AnalysisException {
        def.analyze();
    }

    @Test
    public void testAnalyzeExpection() throws AnalysisException {
        try {
            def = new IndexDef(
                    "index1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxx"
                            + "xxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxinde"
                            + "x1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxx"
                            + "xxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxxindex1xxxxx"
                            + "xxxxxxxxxxxxindex1xxxxxxxxxxxxxxxxx", false,
                    Lists.newArrayList("col1"), IndexDef.IndexType.INVERTED, null,
                    "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e instanceof AnalysisException);
        }
        try {
            def = new IndexDef("", false, Lists.newArrayList("col1"), IndexDef.IndexType.INVERTED, null, "balabala");
            def.analyze();
            Assert.fail("No exception throws.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e instanceof AnalysisException);
        }
        try {
            def = new IndexDef("variant_index", false, Lists.newArrayList("col1"),
                                    IndexDef.IndexType.INVERTED, null, "comment");
            def.checkColumn(new Column("col1", PrimitiveType.VARIANT), KeysType.UNIQUE_KEYS, true, TInvertedIndexFileStorageFormat.V1);
        } catch (AnalysisException e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().contains("not supported in inverted index format V1"));
        }
    }

    @Test
    public void toSql() {
        Assert.assertEquals("INDEX `index1` (`col1`) USING INVERTED COMMENT 'balabala'", def.toSql());
        Assert.assertEquals("INDEX `index1` ON table1 (`col1`) USING INVERTED COMMENT 'balabala'",
                def.toSql("table1"));
    }

    @Test
    public void testArrayTypeSupport() throws AnalysisException {
        def = new IndexDef("array_index", false, Lists.newArrayList("col1"),
                IndexDef.IndexType.INVERTED, null, "array test");

        // Test array of supported types
        Column arrayOfString = new Column("col1",
                ArrayType.create(ScalarType.createVarchar(10), false));
        def.checkColumn(arrayOfString, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);

        Column arrayOfInt = new Column("col1",
                ArrayType.create(ScalarType.createType(PrimitiveType.INT), false));
        def.checkColumn(arrayOfInt, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);

        Column arrayOfDate = new Column("col1",
                ArrayType.create(ScalarType.createType(PrimitiveType.DATE), false));
        def.checkColumn(arrayOfDate, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);

        // Array<Array<String>>
        Column nestedArray = new Column("col1",
                ArrayType.create(ArrayType.create(ScalarType.createVarchar(10), false), false));
        def.checkColumn(nestedArray, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);

        // Test array of unsupported types
        try {
            Column arrayOfFloat = new Column("col1",
                    ArrayType.create(ScalarType.createType(PrimitiveType.FLOAT), false));
            def.checkColumn(arrayOfFloat, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);
            Assert.fail("No exception throws for unsupported array element type.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("is not supported in"));
        }

        try {
            // Array<Map<String, Int>>
            Column arrayOfMap = new Column("col1",
                    ArrayType.create(new MapType(
                            ScalarType.createVarchar(10),
                            ScalarType.createType(PrimitiveType.INT)), false));
            def.checkColumn(arrayOfMap, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);
            Assert.fail("No exception throws for array of map type.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("is not supported in"));
        }

        try {
            // Array<Struct<name:String, age:Int>>
            ArrayList<StructField> fields = new ArrayList<>();
            fields.add(new StructField("name", ScalarType.createVarchar(10), null));
            fields.add(new StructField("age", ScalarType.createType(PrimitiveType.INT), null));
            Column arrayOfStruct = new Column("col1",
                    ArrayType.create(new StructType(fields), false));
            def.checkColumn(arrayOfStruct, KeysType.DUP_KEYS, true, TInvertedIndexFileStorageFormat.V1);
            Assert.fail("No exception throws for array of struct type.");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("is not supported in"));
        }
    }
}

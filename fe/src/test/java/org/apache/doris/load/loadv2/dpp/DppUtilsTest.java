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
//

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.load.loadv2.etl.EtlJobConfig;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.doris.common.jmockit.Deencapsulation;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DppUtilsTest {

    @Test
    public void testDataTypeToClass() {
        DppUtils dppUtils = new DppUtils();

        Class stringResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.StringType);
        Assert.assertEquals(String.class, stringResult);

        Class booleanResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.BooleanType);
        Assert.assertEquals(Boolean.class, booleanResult);

        Class shortResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.ShortType);
        Assert.assertEquals(Short.class, shortResult);

        Class integerResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.IntegerType);
        Assert.assertEquals(Integer.class, integerResult);

        Class longResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.LongType);
        Assert.assertEquals(Long.class, longResult);

        Class floatResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.FloatType);
        Assert.assertEquals(Float.class, floatResult);

        Class doubleResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.DoubleType);
        Assert.assertEquals(Double.class, doubleResult);

        Class dateResult = Deencapsulation.invoke(dppUtils, "dataTypeToClass", DataTypes.DateType);
        Assert.assertEquals(Date.class, dateResult);
    }

    @Test
    public void testColumnTypeToClass() {
        DppUtils dppUtils = new DppUtils();

        Class charResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "CHAR");
        Assert.assertEquals(String.class, charResult);

        Class hllResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "HLL");
        Assert.assertEquals(String.class, hllResult);

        Class objectResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "OBJECT");
        Assert.assertEquals(String.class, objectResult);

        Class booleanResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "BOOL");
        Assert.assertEquals(Boolean.class, booleanResult);

        Class tinyResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "TINYINT");
        Assert.assertEquals(Short.class, tinyResult);

        Class smallResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "SMALLINT");
        Assert.assertEquals(Short.class, smallResult);

        Class integerResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "INT");
        Assert.assertEquals(Integer.class, integerResult);

        Class longResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "BIGINT");
        Assert.assertEquals(Long.class, longResult);

        Class datetimeResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "DATETIME");
        Assert.assertEquals(Long.class, datetimeResult);

        Class floatResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "FLOAT");
        Assert.assertEquals(Float.class, floatResult);

        Class doubleResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "DOUBLE");
        Assert.assertEquals(Double.class, doubleResult);

        Class dateResult = Deencapsulation.invoke(dppUtils, "columnTypeToClass", "DATE");
        Assert.assertEquals(Date.class, dateResult);
    }

    @Test
    public void testColumnTypeToDataType() {
        DppUtils dppUtils = new DppUtils();

        DataType stringResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "VARCHAR");
        Assert.assertEquals(DataTypes.StringType, stringResult);

        DataType charResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "CHAR");
        Assert.assertEquals(DataTypes.StringType, charResult);

        DataType hllResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "HLL");
        Assert.assertEquals(DataTypes.StringType, hllResult);

        DataType objectResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "OBJECT");
        Assert.assertEquals(DataTypes.StringType, objectResult);

        DataType booleanResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "BOOL");
        Assert.assertEquals(DataTypes.BooleanType, booleanResult);

        DataType tinyResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "TINYINT");
        Assert.assertEquals(DataTypes.ShortType, tinyResult);

        DataType smallResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "SMALLINT");
        Assert.assertEquals(DataTypes.ShortType, smallResult);

        DataType integerResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "INT");
        Assert.assertEquals(DataTypes.IntegerType, integerResult);

        DataType longResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "BIGINT");
        Assert.assertEquals(DataTypes.LongType, longResult);

        DataType datetimeResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "DATETIME");
        Assert.assertEquals(DataTypes.LongType, datetimeResult);

        DataType floatResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "FLOAT");
        Assert.assertEquals(DataTypes.FloatType, floatResult);

        DataType doubleResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "DOUBLE");
        Assert.assertEquals(DataTypes.DoubleType, doubleResult);

        DataType dateResult = Deencapsulation.invoke(dppUtils, "columnTypeToDataType", "DATE");
        Assert.assertEquals(DataTypes.DateType, dateResult);
    }

    @Test
    public void testCreateDstTableSchema() {
        DppUtils dppUtils = new DppUtils();

        EtlJobConfig.EtlColumn column1 = new EtlJobConfig.EtlColumn(
                "column1", "INT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        EtlJobConfig.EtlColumn column2 = new EtlJobConfig.EtlColumn(
                "column2", "SMALLINT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        List<EtlJobConfig.EtlColumn> columns = new ArrayList<>();
        columns.add(column1);
        columns.add(column2);

        StructType schema = Deencapsulation.invoke(dppUtils, "createDstTableSchema", columns, false);
        Assert.assertEquals(2, schema.fieldNames().length);
        Assert.assertEquals("column1", schema.fieldNames()[0]);
        Assert.assertEquals("column2", schema.fieldNames()[1]);

        StructType schema2 = Deencapsulation.invoke(dppUtils, "createDstTableSchema", columns, true);
        Assert.assertEquals(3, schema2.fieldNames().length);
        Assert.assertEquals("__bucketId__", schema2.fieldNames()[0]);
        Assert.assertEquals("column1", schema2.fieldNames()[1]);
        Assert.assertEquals("column2", schema2.fieldNames()[2]);
    }

    @Test
    public void testParseColumnsFromPath() {
        DppUtils dppUtils = new DppUtils();

        String path = "/path/to/file/city=beijing/date=2020-04-10/data";
        List<String> columnFromPaths = new ArrayList<>();
        columnFromPaths.add("city");
        columnFromPaths.add("date");
        List<String> columnFromPathValues = Deencapsulation.invoke(dppUtils, "parseColumnsFromPath", path, columnFromPaths);
        Assert.assertEquals(2, columnFromPathValues.size());
        Assert.assertEquals("beijing", columnFromPathValues.get(0));
        Assert.assertEquals("2020-04-10", columnFromPathValues.get(1));
    }
}

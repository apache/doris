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

package org.apache.doris.common.util;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParquetPropertyAnalyzerTest {

    @Test
    public void testParserSchema() {
        // schema is null
        String schema = null;
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("schema is required for parquet file"));
        }
        // schema has less than 3 fields
        schema = "test";
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("must only contains repetition type/data type/column name"));
        }
        // repetition type is error
        schema = "field1, field2, field3";
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("unknown repetition type"));
        }
        // only support required
        schema = "optional, field2, field3";
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently only support required type"));
        }
        // data type is error
        schema = "required, string, field3";
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("data type is not supported"));
        }
        // correct definition of schema
        schema = "required, int32, col1; required, int64, col2;";
        try {
            ParquetPropertyAnalyzer.parseSchema(schema);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testParseFileProperties() {
        Map<String, String> properties = new HashMap<>();
        Set<String> processedKeys = new HashSet<String>();
        Map<String, String> fileProperties = ParquetPropertyAnalyzer.parseFileProperties(properties, processedKeys);
        Assert.assertTrue(fileProperties.size() == 0);
        properties.put("version", "v1");
        fileProperties = ParquetPropertyAnalyzer.parseFileProperties(properties, processedKeys);
        Assert.assertTrue(fileProperties.size() == 0);
        properties.clear();
        properties.put("parquet.version", "v1");
        properties.put("parquet.compression", "snappy");
        fileProperties = ParquetPropertyAnalyzer.parseFileProperties(properties, processedKeys);
        Assert.assertTrue(fileProperties.size() == 2);
        Assert.assertTrue(fileProperties.get("version").equalsIgnoreCase("v1"));
        Assert.assertTrue(fileProperties.get("compression").equalsIgnoreCase("snappy"));
    }

    @Test
    public void testCheckOutExprAndSchema() {
        List<List<String>> schema = new ArrayList<>();
        List<String> column = new ArrayList<>();
        column.add("required");
        column.add("int32");
        column.add("col1");
        schema.add(column);

        List<Expr> outExpr = new ArrayList<>();
        Expr expr1 = new IntLiteral(100);
        outExpr.add(expr1);
        TableName tableName = new TableName("db1", "table1");
        Expr expr2 = new SlotRef(tableName, "col1");
        Deencapsulation.setField(expr2, "type", Type.BIGINT);
        outExpr.add(expr2);
        List<Expr> params = new ArrayList<>();
        params.add(expr2);
        Expr expr3 = new FunctionCallExpr("count", params);
        Deencapsulation.setField(expr3, "type", Type.VARCHAR);
        outExpr.add(expr3);

        // schema size is not equal to out_expr size
        try {
            ParquetPropertyAnalyzer.checkOutExprAndSchema(schema, outExpr);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema number does not equal to select item number"));
        }

        List<String> column2 = new ArrayList<>();
        column2.add("required");
        column2.add("int64");
        column2.add("col2");
        schema.add(column2);

        List<String> column3 = new ArrayList<>();
        column3.add("requried");
        column3.add("boolean");
        column3.add("col3");
        schema.add(column3);

        // schema defined type is different to out_expr defined type
        try {
            ParquetPropertyAnalyzer.checkOutExprAndSchema(schema, outExpr);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is CHAR/VARCHAR/DECIMAL, should use byte_array"));
        }

        schema.remove(2);
        List<String> column4 = new ArrayList<>();
        column4.add("requried");
        column4.add("byte_array");
        column4.add("col3");
        schema.add(column4);

        // correct definition of schema and out_expr
        try {
            ParquetPropertyAnalyzer.checkOutExprAndSchema(schema, outExpr);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testColumnsAndSchema() {
        List<List<String>> schema = new ArrayList<>();
        List<String> column = new ArrayList<>();
        column.add("required");
        column.add("int32");
        column.add("col0");
        schema.add(column);

        List<Column> cols =  new ArrayList<>();
        Column col0 = new Column("col0", PrimitiveType.INT);
        Column col1 = new Column("col1", PrimitiveType.DATE);
        Column col2 = new Column("col2", PrimitiveType.VARCHAR);
        cols.add(col0);
        cols.add(col1);
        cols.add(col2);

        // schema size is not equal to out_expr size
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, cols);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema number does not equal to projection field number"));
        }
        List<String> column2= new ArrayList<>();
        column2.add("required");
        column2.add("int64");
        column2.add("col1");
        schema.add(column2);
        List<String> column3 = new ArrayList<>();
        column3.add("required");
        column3.add("int64");
        column3.add("col2");
        schema.add(column3);

        // schema defined type is different to out_expr defined type
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, cols);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is CHAR/VARCHAR/DECIMAL, should use byte_array"));
        }

        schema.remove(2);
        List<String> column4 = new ArrayList<>();
        column4.add("required");
        column4.add("byte_array");
        column4.add("col2");
        schema.add(column4);
        // correct definition of schema and out_expr
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, cols);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testTableAndSchema() {
        List<List<String>> schema = new ArrayList<>();
        List<String> column = new ArrayList<>();
        column.add("required");
        column.add("int32");
        column.add("col0");
        schema.add(column);

        List<Column> cols =  new ArrayList<>();
        Column col0 = new Column("col0", PrimitiveType.INT);
        Column col1 = new Column("col1", PrimitiveType.DATE);
        Column col2 = new Column("col2", PrimitiveType.VARCHAR);
        cols.add(col0);
        cols.add(col1);
        cols.add(col2);
        Table table = new Table(1001, "table1", Table.TableType.OLAP, cols);

        // schema size is not equal to out_expr size
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, table);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema number does not equal to projection field number"));
        }
        List<String> column2 = new ArrayList<>();
        column2.add("required");
        column2.add("int64");
        column2.add("col1");
        schema.add(column2);
        List<String> column3 = new ArrayList<>();
        column3.add("required");
        column3.add("int64");
        column3.add("col2");
        schema.add(column3);

        // schema defined type is different to out_expr defined type
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, table);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is CHAR/VARCHAR/DECIMAL, should use byte_array"));
        }

        schema.remove(2);
        List<String> column4 = new ArrayList<>();
        column4.add("required");
        column4.add("byte_array");
        column4.add("col2");
        schema.add(column4);
        // correct definition of schema and out_expr
        try {
            ParquetPropertyAnalyzer.checkProjectionFieldAndSchema(schema, table);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSchemaByResultExpr() {
        List<Expr> resultExpr = new ArrayList<>();
        // column expr
        TableName tableName = new TableName("db1", "table1");
        Expr expr1 = new SlotRef(tableName, "col1");
        Deencapsulation.setField(expr1, "type", Type.DECIMALV2);
        resultExpr.add(expr1);

        // literal expr
        Expr expr2 = new IntLiteral(100);
        resultExpr.add(expr2);

        // expr contain function
        FunctionName functionName = new FunctionName("db1", "count");
        List<Expr> params = new ArrayList<>();
        params.add(expr1);
        Expr expr3 = new FunctionCallExpr(functionName, params);
        Deencapsulation.setField(expr3, "type", Type.DECIMALV2);
        resultExpr.add(expr3);

        try {
            List<List<String>> schema = ParquetPropertyAnalyzer.genSchemaByResultExpr(resultExpr);
            Assert.assertTrue(schema.size() == 3);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("col0"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("col1"));
            Assert.assertTrue(schema.get(2).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(2).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(2).get(2).equalsIgnoreCase("col2"));
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            // not support type
            Expr expr4 = new LargeIntLiteral("10000000000");
            resultExpr.add(expr4);
            ParquetPropertyAnalyzer.genSchemaByResultExpr(resultExpr);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently parquet do not support column type"));
        }
    }

    @Test
    public void testGenSchemaByColumns() {
        List<Column> cols =  new ArrayList<>();
        Column col0 = new Column("col0", PrimitiveType.INT);
        Column col1 = new Column("col1", PrimitiveType.DATE);
        Column col2 = new Column("col2", PrimitiveType.VARCHAR);
        cols.add(col0);
        cols.add(col1);
        cols.add(col2);
        try {
            List<List<String>> schema = ParquetPropertyAnalyzer.genSchema(cols);
            Assert.assertTrue(schema.size() == 3);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("col0"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("int64"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("col1"));
            Assert.assertTrue(schema.get(2).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(2).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(2).get(2).equalsIgnoreCase("col2"));
        } catch (Exception e) {
            Assert.fail();
        }

        // not support type
        Column col4 = new Column("col4", PrimitiveType.BIGINT);
        cols.add(col4);
        try {
            ParquetPropertyAnalyzer.genSchema(cols);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently parquet do not support column type"));
        }
    }

    @Test
    public void testGenSchemaByTable() {
        List<Column> cols =  new ArrayList<>();
        Column col0 = new Column("col0", PrimitiveType.INT);
        Column col1 = new Column("col1", PrimitiveType.DATE);
        Column col2 = new Column("col2", PrimitiveType.VARCHAR);
        cols.add(col0);
        cols.add(col1);
        cols.add(col2);
        Table table = new Table(1001, "table1", Table.TableType.OLAP, cols);

        try {
            List<List<String>> schema = ParquetPropertyAnalyzer.genSchema(table);
            Assert.assertTrue(schema.size() == 3);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("col0"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("int64"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("col1"));
            Assert.assertTrue(schema.get(2).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(2).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(2).get(2).equalsIgnoreCase("col2"));
        } catch (Exception e) {
            Assert.fail();
        }

        // not support type
        Column col4 = new Column("col4", PrimitiveType.BIGINT);
        cols.add(col4);
        try {
            ParquetPropertyAnalyzer.genSchema(table);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently parquet do not support column type"));
        }
    }
}

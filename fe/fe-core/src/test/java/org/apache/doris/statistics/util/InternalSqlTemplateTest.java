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

package org.apache.doris.statistics.util;

import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.statistics.util.InternalSqlTemplate.QueryType;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class InternalSqlTemplateTest {

    @Test
    public void testProcessTemplate() throws InvalidFormatException {
        // Setup
        String template = "SELECT * FROM ${table} WHERE id = ${id};";
        String expectSQL = "SELECT * FROM table0 WHERE id = 123;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("id", "123");

        // Run the test
        String result = InternalSqlTemplate.processTemplate(template, params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testProcessTemplate_ThrowsInvalidFormatException() {
        // Setup
        String template = "SELECT * FROM ${table} WHERE id = ${id};";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.processTemplate(template, params));
    }

    @Test
    public void testBuildStatsMinMaxNdvValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value, MAX(column0) AS max_value, "
                + "NDV(column0) AS ndv FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMinMaxNdvValueSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value, MAX(column0) AS max_value, NDV(column0) AS ndv"
                + " FROM table0 TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMinMaxNdvValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsPartitionMinMaxNdvValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value, MAX(column0) AS max_value, NDV(column0) AS ndv"
                + " FROM table0 PARTITIONS (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMinMaxNdvValueSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value, MAX(column0) AS max_value, NDV(column0) AS ndv"
                + " FROM table0 PARTITIONS (partition0) TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMinMaxNdvValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsRowCountSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsRowCountSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsRowCountSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0 TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsRowCountSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsRowCountSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsRowCountSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsPartitionRowCountSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionRowCountSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionRowCountSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0"
                + " PARTITIONS (partition0) TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("partition", "partition0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionRowCountSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionRowCountSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionRowCountSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsMaxAvgSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size, "
                + "AVG(LENGTH(column0)) AS avg_size FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMaxAvgSizeSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMaxAvgSizeSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size, AVG(LENGTH(column0)) AS avg_size"
                + " FROM table0 TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMaxAvgSizeSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMaxAvgSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsMaxAvgSizeSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsPartitionMaxAvgSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size, AVG(LENGTH(column0)) AS avg_size"
                + " FROM table0 PARTITIONS (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMaxAvgSizeSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size, AVG(LENGTH(column0)) AS avg_size"
                + " FROM table0 PARTITIONS (partition0) TABLESAMPLE(20 PERCENT);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMaxAvgSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsNumNullsSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls FROM table0 WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsNumNullsSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsNumNullsSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls FROM table0"
                + " TABLESAMPLE(20 PERCENT) WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsNumNullsSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsNumNullsSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsNumNullsSql(params, QueryType.FULL));
    }

    @Test
    public void testBuildStatsPartitionNumNullsSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls FROM table0"
                + " PARTITIONS (partition0) WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionNumNullsSql(params, QueryType.FULL);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionNumNullsSqlBySample() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls"
                + " FROM table0 PARTITIONS (partition0) TABLESAMPLE(20 PERCENT) WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");
        params.put("percent", "20");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionNumNullsSql(params, QueryType.SAMPLE);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionNumNullsSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionNumNullsSql(params, QueryType.FULL));
    }
}

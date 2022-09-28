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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class InternalSqlTemplateTest {

    @Test
    public void testProcessTemplate() {
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
    public void testBuildStatsMinValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMinValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMinValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsMinValueSql(params));
    }

    @Test
    public void testBuildStatsPartitionMinValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value"
                + " FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMinValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMinValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionMinValueSql(params));
    }

    @Test
    public void testBuildStatsMaxValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(column0) AS max_value FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMaxValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMaxValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsMaxValueSql(params));
    }

    @Test
    public void testBuildStatsPartitionMaxValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(column0) AS max_value FROM"
                + " table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMaxValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMaxValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionMaxValueSql(params));
    }

    @Test
    public void testBuildStatsNdvValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT NDV(column0) AS ndv FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsNdvValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsNdvValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsNdvValueSql(params));
    }

    @Test
    public void testBuildStatsPartitionNdvValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT NDV(column0) AS ndv FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionNdvValueSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionNdvValueSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionNdvValueSql(params));
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
        String result = InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params);

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
                () -> InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params));
    }

    @Test
    public void testBuildStatsPartitionMinMaxNdvValueSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MIN(column0) AS min_value, MAX(column0) AS max_value, "
                + "NDV(column0) AS ndv FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params);

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
                () -> InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params));
    }

    @Test
    public void testBuildStatsRowCountSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsRowCountSql(params);

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
                () -> InternalSqlTemplate.buildStatsRowCountSql(params));
    }

    @Test
    public void testBuildStatsPartitionRowCountSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS row_count FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionRowCountSql(params);

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
                () -> InternalSqlTemplate.buildStatsPartitionRowCountSql(params));
    }

    @Test
    public void testBuildStatsMaxSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsMaxSizeSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsMaxSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsMaxSizeSql(params));
    }

    @Test
    public void testBuildStatsPartitionMaxSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMaxSizeSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionMaxSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionMaxSizeSql(params));
    }

    @Test
    public void testBuildStatsAvgSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT AVG(LENGTH(column0)) AS avg_size FROM table0;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsAvgSizeSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsAvgSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsAvgSizeSql(params));
    }

    @Test
    public void testBuildStatsPartitionAvgSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT AVG(LENGTH(column0)) AS avg_size FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionAvgSizeSql(params);

        // Verify the results
        Assert.assertEquals(expectSQL, result);
    }

    @Test
    public void testBuildStatsPartitionAvgSizeSql_ThrowsInvalidFormatException() {
        // Setup
        Map<String, String> params = new HashMap<>();
        params.put("xxx", "table0");

        // Run the test
        Assert.assertThrows(InvalidFormatException.class,
                () -> InternalSqlTemplate.buildStatsPartitionAvgSizeSql(params));
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
        String result = InternalSqlTemplate.buildStatsMaxAvgSizeSql(params);

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
                () -> InternalSqlTemplate.buildStatsMaxAvgSizeSql(params));
    }

    @Test
    public void testBuildStatsPartitionMaxAvgSizeSql() throws Exception {
        // Setup
        String expectSQL = "SELECT MAX(LENGTH(column0)) AS max_size, "
                + "AVG(LENGTH(column0)) AS avg_size FROM table0 PARTITION (partition0);";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params);

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
                () -> InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params));
    }

    @Test
    public void testBuildStatsNumNullsSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls FROM table0 WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsNumNullsSql(params);

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
                () -> InternalSqlTemplate.buildStatsNumNullsSql(params));
    }

    @Test
    public void testBuildStatsPartitionNumNullsSql() throws Exception {
        // Setup
        String expectSQL = "SELECT COUNT(1) AS num_nulls FROM table0 PARTITION (partition0) WHERE column0 IS NULL;";

        Map<String, String> params = new HashMap<>();
        params.put("table", "table0");
        params.put("column", "column0");
        params.put("partition", "partition0");

        // Run the test
        String result = InternalSqlTemplate.buildStatsPartitionNumNullsSql(params);

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
                () -> InternalSqlTemplate.buildStatsPartitionNumNullsSql(params));
    }
}

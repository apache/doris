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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TableValuedFunctionIfTest {

    @Test
    public void testGetTableColumnsForDescribeStaticSchema() throws AnalysisException {
        // numbers() - static schema, no params needed
        List<Column> cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                NumbersTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("number", cols.get(0).getName());

        // numbers() with params - should still return same static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                NumbersTableValuedFunction.NAME, ImmutableMap.of("number", "10"));
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("number", cols.get(0).getName());

        // backends() - static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                BackendsTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("BackendId", cols.get(0).getName());

        // frontends() - static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                FrontendsTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("Name", cols.get(0).getName());

        // frontends_disks() - static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                FrontendsDisksTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());

        // catalogs() - static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                CatalogsTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());

        // mv_infos() - static schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                MvInfosTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
    }

    @Test
    public void testGetTableColumnsForDescribeSimpleParamTvf() throws AnalysisException {
        // partitions() with catalog param
        Map<String, String> params = Maps.newHashMap();
        params.put("catalog", "internal");
        params.put("database", "test_db");
        params.put("table", "test_tbl");
        List<Column> cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                PartitionsTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());

        // partitions() without catalog param - should still return schema
        params = Maps.newHashMap();
        params.put("database", "test_db");
        params.put("table", "test_tbl");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                PartitionsTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());

        // jobs() with type param
        params = Maps.newHashMap();
        params.put("type", "insert");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                JobsTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());

        // tasks() with type param
        params = Maps.newHashMap();
        params.put("type", "insert");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                TasksTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());

        // hudi_meta() with query_type
        params = Maps.newHashMap();
        params.put("table", "ctl.db.tbl");
        params.put("query_type", "timeline");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HudiTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());
    }

    @Test
    public void testGetTableColumnsForDescribeExternalFileTvf() throws AnalysisException {
        // s3() without csv_schema -> returns __dummy_col
        Map<String, String> params = Maps.newHashMap();
        params.put("URI", "s3://bucket/path");
        params.put("ACCESS_KEY", "ak");
        params.put("SECRET_KEY", "sk");
        List<Column> cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                S3TableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // s3() with csv_schema -> returns parsed columns
        params.put("csv_schema", "k1:int;k2:string");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                S3TableValuedFunction.NAME, params);
        Assert.assertEquals(2, cols.size());
        Assert.assertEquals("k1", cols.get(0).getName());
        Assert.assertEquals("k2", cols.get(1).getName());

        // hdfs() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        params.put("uri", "hdfs://namenode:8020/path");
        params.put("format", "parquet");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HdfsTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // hdfs() with csv_schema
        params.put("csv_schema", "k1:int;k2:bigint");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HdfsTableValuedFunction.NAME, params);
        Assert.assertEquals(2, cols.size());

        // local() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        params.put("filepath", "/tmp/test.parquet");
        params.put("format", "parquet");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                LocalTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // http() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        params.put("url", "http://localhost/test.parquet");
        params.put("format", "parquet");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HttpTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // http_stream() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        params.put("url", "http://localhost/stream");
        params.put("format", "csv");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HttpStreamTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // file() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        params.put("path", "hdfs://namenode:8020/path");
        params.put("format", "parquet");
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                FileTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // cdc_stream() without csv_schema -> returns __dummy_col
        params = Maps.newHashMap();
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                CdcStreamTableValuedFunction.NAME, params);
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        // External file TVFs with empty params -> returns __dummy_col
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                S3TableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());

        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                HdfsTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertEquals(1, cols.size());
        Assert.assertEquals("__dummy_col", cols.get(0).getName());
    }

    @Test
    public void testGetTableColumnsForDescribeParquetMetadata() throws AnalysisException {
        // parquet_metadata
        Map<String, String> params = Maps.newHashMap();
        params.put("path", "hdfs://namenode:8020/test.parquet");
        List<Column> cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                ParquetMetadataTableValuedFunction.NAME, params);
        Assert.assertFalse(cols.isEmpty());

        // parquet_file_metadata
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                ParquetMetadataTableValuedFunction.NAME_FILE_METADATA, params);
        Assert.assertFalse(cols.isEmpty());

        // parquet_kv_metadata
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                ParquetMetadataTableValuedFunction.NAME_KV_METADATA, params);
        Assert.assertFalse(cols.isEmpty());

        // parquet_metadata without path param - should still return schema
        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                ParquetMetadataTableValuedFunction.NAME, ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
    }

    @Test
    public void testGetTableColumnsForDescribeNonExistent() {
        try {
            TableValuedFunctionIf.getTableColumnsForDescribe("nonexistent_tvf", ImmutableMap.of());
            Assert.fail("Should throw AnalysisException for non-existent TVF");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Could not find table function"));
        }
    }

    @Test
    public void testGetTableColumnsForDescribeCaseInsensitive() throws AnalysisException {
        // TVF name should be case-insensitive
        List<Column> cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                "NUMBERS", ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("number", cols.get(0).getName());

        cols = TableValuedFunctionIf.getTableColumnsForDescribe(
                "Backends", ImmutableMap.of());
        Assert.assertFalse(cols.isEmpty());
        Assert.assertEquals("BackendId", cols.get(0).getName());
    }
}

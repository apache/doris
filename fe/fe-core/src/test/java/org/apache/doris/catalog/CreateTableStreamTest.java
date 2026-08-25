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

import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class CreateTableStreamTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
    }

    @Test
    public void testCreateStreamNormalOLAP() throws Exception {
        createDatabase("test_stream");
        // create base sql
        String sql = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true'); ";
        createTable(sql);
        // create default stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s1 on table test_stream.tbl1\n"
                                + "properties('show_initial_rows' = 'true'); "));
        // create append_only stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s2 on table test_stream.tbl1\n"
                                + "properties('type' = 'append_only', 'show_initial_rows' = 'true'); "));
        // create min_delta stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s3 on table test_stream.tbl1\n"
                                + "properties('type' = 'min_delta', 'show_initial_rows' = 'true'); "));

        // create stream already exist
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s1 on table test_stream.tbl1\n"
                                + "properties('show_initial_rows' = 'true'); "));
        dropDatabase("test_stream");
    }

    @Test
    public void testCreateStreamUsesRequestedTypeForBaseValidation() throws Exception {
        createDatabase("test_stream_type_validation");
        String sql = "create table test_stream_type_validation.base_table\n" + "(k1 int, k2 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'false'); ";
        createTable(sql);

        ExceptionChecker.expectThrowsNoException(() ->
                createTable("create stream test_stream_type_validation.append_only_stream "
                        + "on table test_stream_type_validation.base_table\n"
                        + "properties('type' = 'append_only', 'show_initial_rows' = 'false'); "));
        ExceptionChecker.expectThrowsNoException(() ->
                createTable("create stream test_stream_type_validation.detail_stream "
                        + "on table test_stream_type_validation.base_table\n"
                        + "properties('type' = 'detail', 'show_initial_rows' = 'false'); "));

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_stream_type_validation");
        BaseTableStream appendOnlyStream = (BaseTableStream) db.getTableOrDdlException("append_only_stream");
        BaseTableStream detailStream = (BaseTableStream) db.getTableOrDdlException("detail_stream");
        Assertions.assertEquals(BaseTableStream.StreamScanType.APPEND_ONLY,
                appendOnlyStream.getStreamScanType());
        Assertions.assertEquals(BaseTableStream.StreamScanType.DETAIL, detailStream.getStreamScanType());

        String minDeltaError = "MIN_DELTA table stream requires base mow table to enable "
                + "binlog.need_historical_value=true";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, minDeltaError,
                () -> createTable("create stream test_stream_type_validation.default_stream "
                        + "on table test_stream_type_validation.base_table\n"
                        + "properties('show_initial_rows' = 'false'); "));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, minDeltaError,
                () -> createTable("create stream test_stream_type_validation.min_delta_stream "
                        + "on table test_stream_type_validation.base_table\n"
                        + "properties('type' = 'min_delta', 'show_initial_rows' = 'false'); "));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "not supported type: invalid_type",
                () -> createTable("create stream test_stream_type_validation.invalid_type_stream "
                        + "on table test_stream_type_validation.base_table\n"
                        + "properties('type' = 'invalid_type', 'show_initial_rows' = 'false'); "));
        dropDatabase("test_stream_type_validation");
    }

    @Test
    public void testCreateStreamAbnormalOLAP() throws Exception {
        createDatabase("test_stream");
        // create base sql
        String sql = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true'); ";
        createTable(sql);
        // create default stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s1 on table test_stream.tbl1\n"
                                + "properties('show_initial_rows' = 'true'); "));
        // base table not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown table 'tbl2'",
                () -> createTable("create stream if not exists test_stream.s2 on table test_stream.tbl2\n"
                                      + "properties('type' = 'min_delta', 'show_initial_rows' = 'true'); "));
        // stream already exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Table 's1' already exists",
                () -> createTable("create stream test_stream.s1 on table test_stream.tbl1\n"
                                      + "properties('type' = 'min_delta', 'show_initial_rows' = 'true'); "));
        // type not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "not supported type: non_existent_type",
                () -> createTable("create stream if not exists test_stream.s2 on table test_stream.tbl1\n"
                                      + "properties('type' = 'non_existent_type', 'show_initial_rows' = 'true'); "));
        // properties not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown properties: {non_existent_property=a}",
                () -> createTable("create stream if not exists test_stream.s2 on table test_stream.tbl1\n"
                                      + "properties('non_existent_property' = 'a'); "));
        dropDatabase("test_stream");
    }

    @Test
    public void testStreamSchemaFollowsBaseTable() throws Exception {
        createDatabase("test_stream_dynamic_schema");
        createTable("create table test_stream_dynamic_schema.base_table\n"
                + "(k1 int, k2 int)\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'light_schema_change' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true');");
        createTable("create stream test_stream_dynamic_schema.schema_stream "
                + "on table test_stream_dynamic_schema.base_table\n"
                + "properties('show_initial_rows' = 'true');");

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_stream_dynamic_schema");
        BaseTableStream stream = (BaseTableStream) db.getTableOrDdlException("schema_stream");
        Assertions.assertEquals(ImmutableList.of("k1", "k2"), columnNames(stream.getBaseSchema()));
        Assertions.assertEquals(ImmutableList.of("k1", "k2", Column.STREAM_SEQ_COL,
                Column.STREAM_CHANGE_TYPE_COL), columnNames(stream.getFullSchema()));
        Assertions.assertEquals("k2", stream.getColumn("K2").getName());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> stream.setNewFullSchema(ImmutableList.of()));

        JsonObject streamJson = JsonParser.parseString(GsonUtils.GSON.toJson(stream)).getAsJsonObject();
        Assertions.assertTrue(streamJson.getAsJsonArray("fullSchema").isEmpty());

        executeSql("alter table test_stream_dynamic_schema.base_table add column k3 bigint default '0'");
        Assertions.assertEquals(ImmutableList.of("k1", "k2", "k3"), columnNames(stream.getBaseSchema()));
        Assertions.assertEquals(ImmutableList.of("k1", "k2", "k3", Column.STREAM_SEQ_COL,
                Column.STREAM_CHANGE_TYPE_COL), columnNames(stream.getFullSchema()));
        Assertions.assertEquals("k3", stream.getColumn("K3").getName());
        Assertions.assertEquals(ImmutableList.of("k1", "k2", "k3", Column.STREAM_SEQ_COL,
                Column.STREAM_CHANGE_TYPE_COL), columnNames(stream.getColumns()));

        executeSql("alter table test_stream_dynamic_schema.base_table drop column k2");
        Assertions.assertEquals(ImmutableList.of("k1", "k3"), columnNames(stream.getBaseSchema()));
        Assertions.assertNull(stream.getColumn("k2"));

        executeSql("drop table test_stream_dynamic_schema.base_table force");
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                stream::getBaseSchema);
        Assertions.assertEquals("Unknown base table 'base_table'", exception.getMessage());
        Assertions.assertThrows(IllegalStateException.class, stream::getBaseSchemaOrEmpty);
        Assertions.assertThrows(IllegalStateException.class, stream::getFullSchema);
        Assertions.assertThrows(IllegalStateException.class, () -> stream.getColumn("k1"));
        Assertions.assertThrows(IllegalStateException.class, stream::getColumns);

        dropDatabase("test_stream_dynamic_schema");
    }

    private static List<String> columnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }
}

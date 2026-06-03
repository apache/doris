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

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;


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
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1'); ";
        createTable(sql);
        // create default stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s1 on table test_stream.tbl1\n"
                                + "properties('type' = 'default', 'show_initial_rows' = 'true'); "));
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
                                + "properties('type' = 'default', 'show_initial_rows' = 'true'); "));
        dropDatabase("test_stream");
    }

    @Test
    public void testCreateStreamAbnormalOLAP() throws Exception {
        createDatabase("test_stream");
        // create base sql
        String sql = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1'); ";
        createTable(sql);
        // create default stream
        ExceptionChecker
                .expectThrowsNoException(() ->
                        createTable("create stream if not exists test_stream.s1 on table test_stream.tbl1\n"
                                + "properties('type' = 'default', 'show_initial_rows' = 'true'); "));
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
}

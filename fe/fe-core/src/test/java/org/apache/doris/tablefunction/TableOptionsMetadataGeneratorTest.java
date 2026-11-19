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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TSchemaTableName;
import org.apache.doris.thrift.TSchemaTableRequestParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rows of information_schema.table_options for internal OLAP tables: one per partition, and one
 * for a table that has no partition yet.
 */
public class TableOptionsMetadataGeneratorTest extends TestWithFeService {
    private static final String DB_NAME = "table_options_db";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB_NAME);
        // An AUTO PARTITION table has no partition before its first load.
        createTable("create table " + DB_NAME + ".auto_range_no_partition (dt datetime not null,"
                + " user_id bigint not null, cost bigint) unique key(dt, user_id)"
                + " auto partition by range (date_trunc(dt, 'day')) ()"
                + " distributed by hash(user_id) buckets 2 properties('replication_num' = '1')");
        createTable("create table " + DB_NAME + ".list_two_partitions (city varchar(20) not null, cost bigint)"
                + " duplicate key(city) partition by list (city)"
                + " (partition p_bj values in ('Beijing'), partition p_sh values in ('Shanghai'))"
                + " distributed by hash(city) buckets 1 properties('replication_num' = '1')");
    }

    @Test
    public void testTableWithoutPartitionHasOneRow() throws Exception {
        List<TRow> rows = tableOptionsRows("auto_range_no_partition");

        Assertions.assertEquals(1, rows.size());
        List<TCell> row = rows.get(0).getColumnValue();
        Assertions.assertEquals(13, row.size());
        Assertions.assertEquals("UNI", row.get(3).getStringVal()); // TABLE_MODEL
        Assertions.assertEquals("dt,user_id", row.get(4).getStringVal()); // TABLE_MODEL_KEY
        Assertions.assertEquals("user_id", row.get(5).getStringVal()); // DISTRIBUTE_KEY
        Assertions.assertEquals(2, row.get(7).getIntVal()); // BUCKETS_NUM
        Assertions.assertEquals(0, row.get(8).getIntVal()); // PARTITION_NUM
        Assertions.assertEquals("RANGE", row.get(9).getStringVal()); // PARTITION_METHOD
        Assertions.assertEquals("dt", row.get(10).getStringVal()); // PARTITION_EXPRESSION
        Assertions.assertEquals("", row.get(11).getStringVal()); // PARTITION_KEY
        Assertions.assertEquals("", row.get(12).getStringVal()); // RANGE
    }

    @Test
    public void testPartitionedTableHasOneRowPerPartition() throws Exception {
        List<TRow> rows = tableOptionsRows("list_two_partitions");

        Assertions.assertEquals(2, rows.size());
        for (TRow row : rows) {
            Assertions.assertEquals(2, row.getColumnValue().get(8).getIntVal()); // PARTITION_NUM
            Assertions.assertEquals("LIST", row.getColumnValue().get(9).getStringVal()); // PARTITION_METHOD
            Assertions.assertEquals("city", row.getColumnValue().get(10).getStringVal()); // PARTITION_EXPRESSION
        }
        Assertions.assertEquals(ImmutableSet.of("((\"Beijing\"))", "((\"Shanghai\"))"), rows.stream()
                .map(row -> row.getColumnValue().get(11).getStringVal()) // PARTITION_KEY
                .collect(Collectors.toSet()));
    }

    private List<TRow> tableOptionsRows(String tableName) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        TSchemaTableRequestParams params = new TSchemaTableRequestParams();
        params.setCurrentUserIdent(UserIdentity.ROOT.toThrift());
        params.setCatalog(InternalCatalog.INTERNAL_CATALOG_NAME);
        params.setDbId(db.getId());
        TFetchSchemaTableDataResult result = MetadataGenerator.getSchemaTableData(new TFetchSchemaTableDataRequest()
                .setSchemaTableName(TSchemaTableName.TABLE_OPTIONS)
                .setSchemaTableParams(params));

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        return result.getDataBatch().stream()
                .filter(row -> tableName.equals(row.getColumnValue().get(2).getStringVal())) // TABLE_NAME
                .collect(Collectors.toList());
    }
}

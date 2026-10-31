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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.thrift.TVariantSerializeType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The serialize type is internal: DDL creates V2 variant columns and cannot state it, and only metadata older than
 * the field is V1. MODIFY COLUMN keeps what a column is stored with; a created column does not inherit it.
 */
public class VariantSerializeTypeDdlTest extends TestWithFeService {
    private static final String COUNT_ZERO = "VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\")>";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_serialize_type");
        connectContext.setDatabase("variant_serialize_type");
    }

    // DDL cannot create a V1 column, so the metadata is rewritten to what an old image reads as.
    private OlapTable createTableWithOldColumn(String name) throws Exception {
        createTable("CREATE TABLE variant_serialize_type." + name + " (k INT, v " + COUNT_ZERO + ") "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_serialize_type").getTableOrMetaException(name);
        Column column = table.getColumn("v");
        Assertions.assertEquals(TVariantSerializeType.V2, column.getVariantSerializeType());
        column.setType(((VariantType) column.getType()).withSerializeType(TVariantSerializeType.V1));
        return table;
    }

    @Test
    public void testPrintedDdlReplaysOldColumnAsV2() throws Exception {
        OlapTable table = createTableWithOldColumn("t_replay");
        List<String> createTableStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, null, false, true, -1L);
        List<ColumnDefinition> columns = ((CreateTableCommand) new NereidsParser().parseSingle(createTableStmt.get(0)))
                .getCreateTableInfo().getColumnDefinitions();
        Assertions.assertEquals(TVariantSerializeType.V2,
                ((org.apache.doris.nereids.types.VariantType) columns.get(1).getType()).getVariantSerializeType());
    }

    @Test
    public void testModifyColumnKeepsStoredSerializeType() throws Exception {
        OlapTable table = createTableWithOldColumn("t_modify");
        // the restated definition parses as V2
        alterTableSync("ALTER TABLE t_modify MODIFY COLUMN v " + COUNT_ZERO + " COMMENT 'kept'");
        Assertions.assertEquals("kept", table.getColumn("v").getComment());
        Assertions.assertEquals(TVariantSerializeType.V1, table.getColumn("v").getVariantSerializeType());
    }

    @Test
    public void testCtasOfOldColumnCreatesV2() throws Exception {
        createTableWithOldColumn("t_ctas_source");
        CreateTableCommand command = (CreateTableCommand) new NereidsParser().parseSingle(
                "CREATE TABLE t_ctas DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1') "
                        + "AS SELECT * FROM t_ctas_source");
        connectContext.setStatementContext(new StatementContext());
        command.validateCreateTableAsSelect(connectContext, command.getCtasQuery().get());
        List<ColumnDefinition> columns = command.getCreateTableInfo().getColumnDefinitions();
        Assertions.assertEquals(TVariantSerializeType.V2,
                ((org.apache.doris.nereids.types.VariantType) columns.get(1).getType()).getVariantSerializeType());
    }
}

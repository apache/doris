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
 * DDL creates V2 variant columns, SHOW CREATE TABLE states every column's serialize type, and a V1 column with
 * count 0, whose layout it decides, cannot change it.
 */
public class VariantSerializeTypeDdlTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_serialize_type");
        connectContext.setDatabase("variant_serialize_type");
    }

    @Test
    public void testDdlDefaultsToV2AndShowCreateTableStatesSerializeType() throws Exception {
        createTable("CREATE TABLE variant_serialize_type.t (k INT, "
                + "z VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\")>, "
                + "l VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\", "
                + "\"variant_serialize_type\" = \"v1\")>, "
                + "c VARIANT<PROPERTIES(\"variant_serialize_type\" = \"v1\")>) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_serialize_type").getTableOrMetaException("t");
        Assertions.assertEquals(TVariantSerializeType.V2, table.getColumn("z").getVariantSerializeType());
        Assertions.assertEquals(TVariantSerializeType.V1, table.getColumn("l").getVariantSerializeType());
        Assertions.assertEquals(TVariantSerializeType.V1, table.getColumn("c").getVariantSerializeType());

        // the printed DDL states every column's serialize type, so each one replays with its layout
        List<String> createTableStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, null, false, true, -1L);
        String ddl = createTableStmt.get(0);
        List<ColumnDefinition> columns = ((CreateTableCommand) new NereidsParser().parseSingle(ddl))
                .getCreateTableInfo().getColumnDefinitions();
        Assertions.assertEquals(TVariantSerializeType.V2,
                ((org.apache.doris.nereids.types.VariantType) columns.get(1).getType()).getVariantSerializeType());
        Assertions.assertEquals(TVariantSerializeType.V1,
                ((org.apache.doris.nereids.types.VariantType) columns.get(2).getType()).getVariantSerializeType());
        Assertions.assertEquals(TVariantSerializeType.V1,
                ((org.apache.doris.nereids.types.VariantType) columns.get(3).getType()).getVariantSerializeType());

        // a V1 count-0 column cannot become V2 count 0, whose layout is the opposite
        Exception error = Assertions.assertThrows(Exception.class, () -> alterTableSync(
                "ALTER TABLE t MODIFY COLUMN l VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\")>"));
        Assertions.assertTrue(error.getMessage().contains("Can not change variant serialize type"),
                error.getMessage());
        // restating the property, as the printed DDL does, keeps it V1
        alterTableSync("ALTER TABLE t MODIFY COLUMN l VARIANT<PROPERTIES(\"variant_max_subcolumns_count\" = \"0\", "
                + "\"variant_serialize_type\" = \"v1\")> COMMENT 'kept'");
        Assertions.assertEquals(TVariantSerializeType.V1, table.getColumn("l").getVariantSerializeType());
        // a V1 column with any other count stores the same way as a bare VARIANT, so it can be restated as one
        alterTableSync("ALTER TABLE t MODIFY COLUMN c VARIANT COMMENT 'restated'");
        Assertions.assertEquals("restated", table.getColumn("c").getComment());
    }
}

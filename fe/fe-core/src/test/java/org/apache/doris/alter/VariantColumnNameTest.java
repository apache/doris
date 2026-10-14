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

package org.apache.doris.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VariantColumnNameTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("variant_column_name");
        connectContext.setDatabase("variant_column_name");
    }

    @Test
    public void testRejectDottedVariantColumnNames() throws Exception {
        createTable("CREATE TABLE variant_column_name.t (k INT, v VARIANT, `s.x` STRING) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog()
                .getDbOrMetaException("variant_column_name").getTableOrMetaException("t");
        String[] statements = {
                "ALTER TABLE t ADD COLUMN `v.x` VARIANT",
                "ALTER TABLE t ADD COLUMN (good VARIANT, `v.x` VARIANT)",
                "ALTER TABLE t MODIFY COLUMN `s.x` VARIANT",
                "ALTER TABLE t RENAME COLUMN v `v.x`"
        };
        for (String statement : statements) {
            Exception error = Assertions.assertThrows(Exception.class, () -> alterTableSync(statement));
            Assertions.assertTrue(error.getMessage().contains(
                    "`VARIANT` type column named with a `.` character"), error.getMessage());
            Assertions.assertEquals(3, table.getBaseSchema().size());
            Assertions.assertNull(table.getColumn("v.x"));
            Assertions.assertNull(table.getColumn("good"));
            Assertions.assertTrue(table.getColumn("s.x").getType().isStringType());
            Assertions.assertNotNull(table.getColumn("v"));
        }
        Assertions.assertThrows(Exception.class, () -> createTable(
                "CREATE TABLE bad (k INT, `v.x` VARIANT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')"));
        alterTableSync("ALTER TABLE t ADD COLUMN good VARIANT");
        Assertions.assertTrue(table.getColumn("good").getType().isVariantType());
        alterTableSync("ALTER TABLE t RENAME COLUMN good renamed");
        Assertions.assertTrue(table.getColumn("renamed").getType().isVariantType());
        alterTableSync("ALTER TABLE t ADD COLUMN `other.x` STRING");
        Assertions.assertNotNull(table.getColumn("other.x"));
        alterTableSync("ALTER TABLE t RENAME COLUMN `other.x` `other.y`");
        Assertions.assertNotNull(table.getColumn("other.y"));
    }
}

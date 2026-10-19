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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.Config;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.WarmupSelectCommand;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class WarmupSelectIndexCommandTest {
    @Test
    void reusesWarmupSelectForIndexOnlyMode() {
        Assertions.assertInstanceOf(WarmupSelectCommand.class, new NereidsParser().parseSingle(
                "WARM UP SELECT * FROM lake.demo.items PROPERTIES (\"read_index_only\" = true)"));
    }

    @Test
    void preservesDataModeAndItsFileCacheRequirement() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        String cloud = Config.cloud_unique_id;
        try {
            Config.cloud_unique_id = "";
            context.getSessionVariable().setEnableFileCache(true);
            for (String suffix : new String[] {"", " PROPERTIES (read_index_only=false)"}) {
                WarmupSelectCommand command = (WarmupSelectCommand) new NereidsParser().parseSingle(
                        "WARM UP SELECT settings FROM items WHERE settings > 0" + suffix);
                Assertions.assertEquals(6, command.getResultSetMetaData().getColumns().size());
            }
            context.getSessionVariable().setEnableFileCache(false);
            Assertions.assertThrows(Exception.class,
                    () -> new NereidsParser().parseSingle("WARM UP SELECT * FROM items"));
            WarmupSelectCommand index = (WarmupSelectCommand) new NereidsParser().parseSingle(
                    "WARM UP SELECT embedding FROM items PROPERTIES (read_index_only=true)");
            Assertions.assertEquals(5, index.getResultSetMetaData().getColumns().size());
        } finally {
            Config.cloud_unique_id = cloud;
            ConnectContext.remove();
        }
    }

    @Test
    void supportsQualifiedColumnsAndStars() {
        for (String query : new String[] {
                "SELECT i.embedding FROM lake.demo.items i",
                "SELECT items.* FROM lake.demo.items",
                "SELECT embedding, embedding FROM lake.demo.items",
                "SELECT properties FROM lake.demo.items properties",
                "SELECT settings FROM lake.demo.items settings"}) {
            Assertions.assertInstanceOf(WarmupSelectCommand.class, new NereidsParser().parseSingle(
                    "WARM UP " + query + " PROPERTIES (read_index_only='TRUE')"));
        }
    }

    @Test
    void rejectsUnsupportedPropertiesAndProjectionSemantics() {
        for (String sql : new String[] {
                "WARM UP INDEX idx ON lake.demo.items",
                "WARM UP SELECT * FROM items SETTINGS (read_index_only=true)",
                "WARM UP SELECT * FROM items PROPERTIES (read_index_only='yes')",
                "WARM UP SELECT * FROM items PROPERTIES (typo=true)",
                "WARM UP SELECT * FROM items WHERE id=1 PROPERTIES (read_index_only=true)",
                "EXPLAIN WARM UP SELECT * FROM items PROPERTIES (read_index_only=true)",
                "WARM UP SELECT wrong.embedding FROM items PROPERTIES (read_index_only=true)",
                "WARM UP SELECT * EXCEPT(id) FROM items PROPERTIES (read_index_only=true)",
                "WARM UP SELECT *, id FROM items PROPERTIES (read_index_only=true)",
                "WARM UP SELECT id+1 FROM items PROPERTIES (read_index_only=true)"}) {
            Assertions.assertThrows(Exception.class, () -> new NereidsParser().parseSingle(sql), sql);
        }
    }
}

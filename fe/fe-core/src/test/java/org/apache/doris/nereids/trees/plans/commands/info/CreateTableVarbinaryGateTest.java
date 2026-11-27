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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CreateTableVarbinaryGateTest extends TestWithFeService {
    private static final String OLD_ERROR = "doris do not support varbinary create table";

    private final AtomicBoolean declares = new AtomicBoolean(true);
    private final AtomicInteger capabilityChecks = new AtomicInteger();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("db1");
        useDatabase("db1");
        // a plain subclass needs no metastore and no Mockito
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(990001L, "plugin_ctl", null,
                new HashMap<>(), "", null) {
            @Override
            public boolean hasConnectorCapability(ConnectorCapability capability) {
                capabilityChecks.incrementAndGet();
                return capability == ConnectorCapability.SUPPORTS_VARBINARY_COLUMN && declares.get();
            }
        };
        Map<String, CatalogIf> byName = Deencapsulation.getField(Env.getCurrentEnv().getCatalogMgr(), "nameToCatalog");
        byName.put("plugin_ctl", catalog);
    }

    private void validate(String ddl) {
        ((CreateTableCommand) new NereidsParser().parseSingle(ddl)).getCreateTableInfo().validate(connectContext);
    }

    @Test
    public void declaringCatalogAcceptsTopLevelAndNestedVarbinary() {
        for (String cols : new String[] {"b varbinary", "b varbinary(16)", "b array<varbinary>",
                "b map<string, varbinary>", "b map<varbinary, string>", "b struct<x:varbinary, y:int>"}) {
            Assertions.assertDoesNotThrow(() -> validate("create table plugin_ctl.db1.t (id int, " + cols + ")"),
                    cols);
        }
        // masking VARBINARY must not skip the other type checks
        Assertions.assertThrows(AnalysisException.class,
                () -> validate("create table plugin_ctl.db1.t (id int, b map<array<varbinary>, int>)"));
        Assertions.assertThrows(AnalysisException.class, () -> validate("create table plugin_ctl.db1.t (id int, "
                + "b array<array<array<array<array<array<array<array<array<array<varbinary>>>>>>>>>>)"));
    }

    @Test
    public void nonDeclaringCatalogKeepsTheOldErrors() {
        declares.set(false);
        try {
            AnalysisException top = Assertions.assertThrows(AnalysisException.class,
                    () -> validate("create table plugin_ctl.db1.t (id int, b varbinary)"));
            Assertions.assertTrue(top.getMessage().contains(OLD_ERROR), top.getMessage());
            AnalysisException nested = Assertions.assertThrows(AnalysisException.class,
                    () -> validate("create table plugin_ctl.db1.t (id int, b array<varbinary>)"));
            Assertions.assertTrue(nested.getMessage().contains("unsupported sub-type"), nested.getMessage());
        } finally {
            declares.set(true);
        }
    }

    @Test
    public void internalOlapKeepsTheOldErrorsWithoutAskingAnyConnector() {
        capabilityChecks.set(0);
        AnalysisException error = Assertions.assertThrows(AnalysisException.class, () -> validate(
                "create table internal.db1.o (k1 int, b varbinary) duplicate key(k1) distributed by hash(k1) buckets 1"));
        Assertions.assertTrue(error.getMessage().contains(OLD_ERROR), error.getMessage());
        Assertions.assertEquals(0, capabilityChecks.get());
    }

    @Test
    public void capabilityIsCheckedOnlyWhenAColumnHasVarbinary() {
        capabilityChecks.set(0);
        validate("create table plugin_ctl.db1.t (id int, s string)");
        Assertions.assertEquals(0, capabilityChecks.get(), "no VARBINARY must not force the catalog to initialize");
    }

    @Test
    public void ctasGoesThroughTheSameGate() {
        List<ColumnDefinition> cols = Lists.newArrayList(
                new ColumnDefinition("k1", IntegerType.INSTANCE, true),
                new ColumnDefinition("b", VarBinaryType.INSTANCE, true),
                new ColumnDefinition("a", ArrayType.of(VarBinaryType.INSTANCE), true));
        CreateTableInfo info = ((CreateTableCommand) new NereidsParser().parseSingle(
                "create table plugin_ctl.db1.c as select 1 as k1")).getCreateTableInfo();
        Assertions.assertDoesNotThrow(() -> info.validateCreateTableAsSelect(
                Lists.newArrayList("plugin_ctl", "db1", "c"), cols, connectContext));

        declares.set(false);
        try {
            CreateTableInfo rejected = ((CreateTableCommand) new NereidsParser().parseSingle(
                    "create table plugin_ctl.db1.c as select 1 as k1")).getCreateTableInfo();
            Assertions.assertThrows(AnalysisException.class, () -> rejected.validateCreateTableAsSelect(
                    Lists.newArrayList("plugin_ctl", "db1", "c"), cols, connectContext));
        } finally {
            declares.set(true);
        }
    }
}

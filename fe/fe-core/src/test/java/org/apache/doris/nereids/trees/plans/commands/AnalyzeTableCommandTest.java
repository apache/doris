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

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.analysis.AnalysisInfo;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AnalyzeTableCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(ctx.getState()).thenReturn(new QueryState());
        Mockito.when(ctx.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(ctx.getNameSpaceContext()).thenReturn(new NameSpaceContext(internalCtl, null, -1));
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    void testAnalyzeMixedSpatialColumns() throws AnalysisException {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Set<Column> columns = Set.of(new Column("id", Type.INT),
                new Column("geom", ScalarType.createGeometryType("EPSG:3857")),
                new Column("geog", ScalarType.createGeographyType("projjson:my_crs", "vincenty")));
        Mockito.when(table.getSchemaAllIndexes(false)).thenReturn(columns);
        for (Column column : columns) {
            Mockito.when(table.getColumn(column.getName())).thenReturn(column);
        }
        Mockito.when(accessManager.checkTblPriv(Mockito.eq(ctx), Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);
        TableNameInfo name = new TableNameInfo(internalCtl, "db", "mixed");
        AnalyzeTableCommand implicit = new AnalyzeTableCommand(name, null, null,
                new AnalyzeProperties(defaultAnalyzeProperties()));
        Deencapsulation.setField(implicit, "table", table);
        implicit.check();
        Assertions.assertEquals(Set.of("id"), implicit.getColumnNames());

        SessionVariable session = ctx.getSessionVariable();
        session.enableAnalyzeComplexTypeColumn = false;
        AnalyzeTableCommand explicit = new AnalyzeTableCommand(name, null, ImmutableList.of("id", "geom", "geog"),
                new AnalyzeProperties(defaultAnalyzeProperties()));
        Deencapsulation.setField(explicit, "table", table);
        explicit.check();
        Assertions.assertEquals(Set.of("id"), explicit.getColumnNames());

        session.enableAnalyzeComplexTypeColumn = true;
        AnalyzeTableCommand rejected = new AnalyzeTableCommand(name, null, ImmutableList.of("geom", "geog"),
                new AnalyzeProperties(defaultAnalyzeProperties()));
        Deencapsulation.setField(rejected, "table", table);
        Assertions.assertThrows(AnalysisException.class, rejected::check);
    }

    @Test
    void testCheckAnalyzePrivilege() {
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL_NAME),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);

        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL2_NAME),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        AnalyzeTableCommand analyzeTableCommand = new AnalyzeTableCommand(tableNameInfo,
                partitionNamesInfo, ImmutableList.of("k1"), AnalyzeProperties.DEFAULT_PROP);
        // normal
        Assertions.assertDoesNotThrow(() -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo));

        // no privilege
        TableNameInfo tableNameInfo2 = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertThrows(AnalysisException.class,
                () -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo2),
                "ANALYZE command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }

    @Test
    void testCollectHotValueDefaultAndOverride() {
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        AnalyzeTableCommand fullAnalyzeCommand = new AnalyzeTableCommand(tableNameInfo,
                null, null, new AnalyzeProperties(defaultAnalyzeProperties()));
        Assertions.assertFalse(fullAnalyzeCommand.getAnalyzeProperties().hasCollectHotValue());
        Assertions.assertEquals(AnalysisInfo.AnalysisMethod.FULL, fullAnalyzeCommand.getAnalysisMethod());

        Map<String, String> sampleProperties = defaultAnalyzeProperties();
        sampleProperties.put(AnalyzeProperties.PROPERTY_SAMPLE_ROWS, "100");
        AnalyzeTableCommand sampleAnalyzeCommand = new AnalyzeTableCommand(tableNameInfo,
                null, null, new AnalyzeProperties(sampleProperties));
        Assertions.assertFalse(sampleAnalyzeCommand.getAnalyzeProperties().hasCollectHotValue());
        Assertions.assertEquals(AnalysisInfo.AnalysisMethod.SAMPLE, sampleAnalyzeCommand.getAnalysisMethod());

        Map<String, String> forcedFullHotValueProperties = defaultAnalyzeProperties();
        forcedFullHotValueProperties.put(AnalyzeProperties.PROPERTY_COLLECT_HOT_VALUE, "true");
        AnalyzeTableCommand forcedFullHotValueCommand = new AnalyzeTableCommand(tableNameInfo,
                null, null, new AnalyzeProperties(forcedFullHotValueProperties));
        Assertions.assertTrue(forcedFullHotValueCommand.getAnalyzeProperties().collectHotValue());
    }

    @Test
    void testCollectHotValuePropertyValidation() {
        Map<String, String> properties = defaultAnalyzeProperties();
        properties.put(AnalyzeProperties.PROPERTY_COLLECT_HOT_VALUE, "invalid");
        AnalyzeProperties analyzeProperties = new AnalyzeProperties(properties);
        Assertions.assertThrows(AnalysisException.class, analyzeProperties::check);
    }

    @Test
    void testParseWithHotValue() {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle("ANALYZE TABLE test_db.test_tbl WITH HOT VALUE");
        Assertions.assertTrue(plan instanceof AnalyzeTableCommand);
        AnalyzeTableCommand command = (AnalyzeTableCommand) plan;
        Assertions.assertTrue(command.getAnalyzeProperties().hasCollectHotValue());
        Assertions.assertTrue(command.getAnalyzeProperties().collectHotValue());
    }

    @Test
    void testSampleAnalyzeWithHotValueRejected() {
        NereidsParser parser = new NereidsParser();
        AnalyzeTableCommand command = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE test_db.test_tbl WITH SAMPLE ROWS 100 WITH HOT VALUE");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> command.validate(ctx));
        Assertions.assertEquals("Sample analyze does not support WITH HOT VALUE", exception.getDetailMessage());
    }

    private Map<String, String> defaultAnalyzeProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AnalyzeProperties.PROPERTY_SYNC, "false");
        properties.put(AnalyzeProperties.PROPERTY_ANALYSIS_TYPE, "FUNDAMENTALS");
        return properties;
    }
}

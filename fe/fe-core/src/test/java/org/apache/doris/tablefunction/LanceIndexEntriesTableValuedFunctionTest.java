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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.BuiltinTableValuedFunctions;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TLanceIndexMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LanceIndexEntriesTableValuedFunctionTest {
    private static final List<String> PINNED_COLUMN_NAMES = Arrays.asList(
            "CatalogName", "DatabaseName", "TableName", "IndexName", "IndexUuid", "DatasetVersion");

    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext context;
    @Mocked
    private CatalogMgr catalogMgr;
    @Mocked
    private LanceExternalCatalog lanceCatalog;
    @Mocked
    private CatalogIf nonLanceCatalog;
    @Mocked
    private ExternalDatabase database;
    @Mocked
    private LanceExternalTable lanceTable;
    @Mocked
    private TableIf nonLanceTable;

    @Test
    public void testSchemaHasExactlySixPinnedColumnsInOrder() {
        List<Column> schema = LanceIndexEntriesTableValuedFunction.getSchemaForTest();

        Assert.assertEquals(PINNED_COLUMN_NAMES, schema.stream().map(Column::getName)
                .collect(java.util.stream.Collectors.toList()));
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(PrimitiveType.STRING, schema.get(i).getDataType());
            Assert.assertFalse(schema.get(i).isAllowNull());
        }
        Assert.assertEquals(PrimitiveType.BIGINT, schema.get(5).getDataType());
        Assert.assertTrue(schema.get(5).isAllowNull());
    }

    @Test
    public void testColumnIndexRoundTripCaseInsensitive() {
        for (int i = 0; i < PINNED_COLUMN_NAMES.size(); i++) {
            String name = PINNED_COLUMN_NAMES.get(i);
            Assert.assertEquals(Integer.valueOf(i),
                    LanceIndexEntriesTableValuedFunction.getColumnIndexFromColumnName(name));
            Assert.assertEquals(Integer.valueOf(i),
                    LanceIndexEntriesTableValuedFunction.getColumnIndexFromColumnName(
                            name.toLowerCase(java.util.Locale.ROOT)));
            Assert.assertEquals(Integer.valueOf(i),
                    LanceIndexEntriesTableValuedFunction.getColumnIndexFromColumnName(
                            name.toUpperCase(java.util.Locale.ROOT)));
        }
        Assert.assertNull(
                LanceIndexEntriesTableValuedFunction.getColumnIndexFromColumnName("NoSuchColumn"));
    }

    @Test
    public void testMissingTablePropertyRejected() {
        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.normalizeProperties(
                        new LinkedHashMap<>()));

        Map<String, String> blank = new LinkedHashMap<>();
        blank.put("table", "   ");
        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.normalizeProperties(blank));
    }

    @Test
    public void testDuplicateTablePropertyCaseVariantRejected() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("TABLE", null);
        properties.put("table", "ctl.db.tbl");

        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.normalizeProperties(properties));
    }

    @Test
    public void testUnknownPropertyRejected() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table", "ctl.db.tbl");
        properties.put("deadline", "1");

        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.normalizeProperties(properties));
    }

    @Test
    public void testTwoPartNameRejected() {
        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.parseTableName("db.tbl"));
    }

    @Test
    public void testFourPartNameRejected() {
        Assert.assertThrows(AnalysisException.class,
                () -> LanceIndexEntriesTableValuedFunction.parseTableName("lance_catalog.doris.analytics.items"));
    }

    @Test
    public void testBacktickQuotedDottedNamesParse() throws Exception {
        TableName name = LanceIndexEntriesTableValuedFunction.parseTableName(
                "lance_catalog.`doris.analytics`.`my.items`");

        Assert.assertEquals("lance_catalog", name.getCtl());
        Assert.assertEquals("doris.analytics", name.getDb());
        Assert.assertEquals("my.items", name.getTbl());
    }

    @Test
    public void testNonLanceCatalogRejected() {
        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = nonLanceCatalog;
            }
        };

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new LanceIndexEntriesTableValuedFunction(
                        ImmutableMap.of("table", "ctl.db.tbl")));
        Assert.assertTrue(exception.getMessage().contains("is not a Lance catalog"));
    }

    @Test
    public void testNonLanceTableInLanceCatalogRejected() throws Exception {
        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = lanceCatalog;
                lanceCatalog.isRestCatalogConfigured();
                result = false;
                lanceCatalog.getDbOrAnalysisException("db");
                result = database;
                database.getTableOrAnalysisException("tbl");
                result = nonLanceTable;
            }
        };

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new LanceIndexEntriesTableValuedFunction(
                        ImmutableMap.of("table", "ctl.db.tbl")));
        Assert.assertTrue(exception.getMessage().contains("is not a Lance table"));
    }

    @Test
    public void testRestCatalogRejectedBeforeDatabaseResolution() throws Exception {
        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = lanceCatalog;
                lanceCatalog.isRestCatalogConfigured();
                result = true;
            }
        };

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new LanceIndexEntriesTableValuedFunction(
                        ImmutableMap.of("table", "ctl.db.tbl")));
        Assert.assertEquals("lance_index_entries is not supported for Lance REST catalogs",
                exception.getMessage());

        new Verifications() {
            {
                lanceCatalog.getDbOrAnalysisException(anyString);
                times = 0;
            }
        };
    }

    @Test
    public void testGetMetaScanRangeCarriesTableIdentity() throws Exception {
        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = lanceCatalog;
                lanceCatalog.isRestCatalogConfigured();
                result = false;
                lanceCatalog.getDbOrAnalysisException("db");
                result = database;
                database.getTableOrAnalysisException("tbl");
                result = lanceTable;
            }
        };

        LanceIndexEntriesTableValuedFunction tvf = new LanceIndexEntriesTableValuedFunction(
                ImmutableMap.of("TABLE", "  ctl.db.tbl  "));
        Assert.assertEquals("ctl", tvf.getCatalogName());
        Assert.assertEquals("db", tvf.getDatabaseName());
        Assert.assertEquals("tbl", tvf.getSourceTableName());
        Assert.assertEquals(TMetadataType.LANCE_INDEX_ENTRIES, tvf.getMetadataType());

        TMetaScanRange scanRange = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertEquals(TMetadataType.LANCE_INDEX_ENTRIES, scanRange.getMetadataType());
        Assert.assertTrue(scanRange.isSetLanceIndexParams());
        TLanceIndexMetadataParams params = scanRange.getLanceIndexParams();
        Assert.assertEquals("ctl", params.getCatalog());
        Assert.assertEquals("db", params.getDatabase());
        Assert.assertEquals("tbl", params.getTable());

        new Verifications() {
            {
                lanceTable.loadIndexEntries();
                times = 0;
            }
        };
    }

    @Test
    public void testRegistrationWiring() throws Exception {
        Assert.assertTrue(BuiltinTableValuedFunctions.INSTANCE.tableValuedFunctions.stream()
                .anyMatch(func -> func.names.contains(LanceIndexEntriesTableValuedFunction.NAME)));

        Assert.assertEquals(Integer.valueOf(4),
                MetadataTableValuedFunction.getColumnIndexFromColumnName(
                        TMetadataType.LANCE_INDEX_ENTRIES, "indexuuid", null));

        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = lanceCatalog;
                lanceCatalog.isRestCatalogConfigured();
                result = false;
                lanceCatalog.getDbOrAnalysisException("db");
                result = database;
                database.getTableOrAnalysisException("tbl");
                result = lanceTable;
            }
        };

        TableValuedFunctionIf tvf = TableValuedFunctionIf.getTableFunction(
                LanceIndexEntriesTableValuedFunction.NAME, ImmutableMap.of("table", "ctl.db.tbl"));
        Assert.assertTrue(tvf instanceof LanceIndexEntriesTableValuedFunction);
    }
}

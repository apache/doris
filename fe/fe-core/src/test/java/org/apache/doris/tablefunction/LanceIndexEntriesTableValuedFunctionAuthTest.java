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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.LancePhysicalIndexEntry;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFetchSchemaTableDataRequest;
import org.apache.doris.thrift.TFetchSchemaTableDataResult;
import org.apache.doris.thrift.TLanceIndexMetadataParams;
import org.apache.doris.thrift.TMetadataTableRequestParams;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUserIdentity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Assert;
import org.junit.Test;

public class LanceIndexEntriesTableValuedFunctionAuthTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext context;
    @Mocked
    private CatalogMgr catalogMgr;
    @Mocked
    private LanceExternalCatalog catalog;
    @Mocked
    private ExternalDatabase database;
    @Mocked
    private LanceExternalTable table;

    @Test
    public void testShowDeniedBeforeCatalogLookup() {
        new Expectations() {
            {
                ConnectContext.get();
                result = context;
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(context,
                        withInstanceOf(TableName.class), PrivPredicate.SHOW);
                result = false;
                context.getQualifiedUser();
                result = "denied_user";
                context.getRemoteIP();
                result = "127.0.0.1";
            }
        };

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new LanceIndexEntriesTableValuedFunction(
                        ImmutableMap.of("table", "ctl.db.tbl")));
        Assert.assertTrue(exception.getMessage().contains("denied"));

        new Verifications() {
            {
                env.getCatalogMgr();
                times = 0;
            }
        };
    }

    @Test
    public void testMasterRejectsMissingLanceParams() throws Exception {
        TMetadataTableRequestParams params = new TMetadataTableRequestParams()
                .setMetadataType(TMetadataType.LANCE_INDEX_ENTRIES);
        TFetchSchemaTableDataRequest request = new TFetchSchemaTableDataRequest()
                .setMetadaTableParams(params);

        TFetchSchemaTableDataResult result = MetadataGenerator.getMetadataTable(request);

        Assert.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assert.assertTrue(result.getStatus().getErrorMsgs().toString()
                .contains("Lance index metadata params is not set."));
        Assert.assertFalse(result.isSetDataBatch());
    }

    @Test
    public void testMasterRepeatsShowCheckBeforeCatalogLookup() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(withInstanceOf(UserIdentity.class),
                        "ctl", "db", "tbl", PrivPredicate.SHOW);
                result = false;
            }
        };

        TFetchSchemaTableDataResult result = MetadataGenerator.lanceIndexEntriesMetadataResult(
                masterParams("ctl", "db", "tbl"));

        Assert.assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatusCode());
        Assert.assertTrue(result.getStatus().getErrorMsgs().toString().contains("denied"));
        Assert.assertFalse(result.isSetDataBatch());
        new Verifications() {
            {
                env.getCatalogMgr();
                times = 0;
            }
        };
    }

    @Test
    public void testMasterBuildsSixCellRowsFromResolvedNames() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                result = env;
                times = 2;
                env.getAccessManager();
                result = accessManager;
                accessManager.checkTblPriv(withInstanceOf(UserIdentity.class),
                        "ctl", "db", "tbl", PrivPredicate.SHOW);
                result = true;
                env.getCatalogMgr();
                result = catalogMgr;
                catalogMgr.getCatalog("ctl");
                result = catalog;
                catalog.isRestCatalogConfigured();
                result = false;
                catalog.getDbOrAnalysisException("db");
                result = database;
                database.getTableOrAnalysisException("tbl");
                result = table;
                catalog.getName();
                result = "resolved_catalog";
                database.getFullName();
                result = "resolved_db";
                table.getName();
                result = "resolved_table";
                table.loadIndexEntries();
                result = ImmutableList.of(new LancePhysicalIndexEntry("idx", "uuid-1", 7));
            }
        };

        TFetchSchemaTableDataResult result = MetadataGenerator.lanceIndexEntriesMetadataResult(
                masterParams("ctl", "db", "tbl"));

        Assert.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        Assert.assertEquals(1, result.getDataBatchSize());
        TRow row = result.getDataBatch().get(0);
        Assert.assertEquals(6, row.getColumnValueSize());
        Assert.assertEquals("resolved_catalog", row.getColumnValue().get(0).getStringVal());
        Assert.assertEquals("resolved_db", row.getColumnValue().get(1).getStringVal());
        Assert.assertEquals("resolved_table", row.getColumnValue().get(2).getStringVal());
        Assert.assertEquals("idx", row.getColumnValue().get(3).getStringVal());
        Assert.assertEquals("uuid-1", row.getColumnValue().get(4).getStringVal());
        Assert.assertEquals(7, row.getColumnValue().get(5).getLongVal());
    }

    private static TMetadataTableRequestParams masterParams(
            String catalogName, String databaseName, String tableName) {
        TLanceIndexMetadataParams lanceParams = new TLanceIndexMetadataParams()
                .setCatalog(catalogName)
                .setDatabase(databaseName)
                .setTable(tableName);
        TUserIdentity user = new TUserIdentity()
                .setUsername("denied_user")
                .setHost("127.0.0.1");
        return new TMetadataTableRequestParams()
                .setMetadataType(TMetadataType.LANCE_INDEX_ENTRIES)
                .setLanceIndexMetadataParams(lanceParams)
                .setCurrentUserIdent(user);
    }
}

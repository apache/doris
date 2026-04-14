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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests for RefreshMTMVInfo.analyze() constraint validation of INCREMENTAL/PARTITIONS modes.
 */
public class RefreshMTMVInfoAnalyzeTest {

    private MockedStatic<Env> mockedEnvStatic;

    private Env env;
    private InternalCatalog catalog;
    private Database db;
    private ConnectContext ctx;
    private AccessControllerManager accessManager;

    private MTMV ivmCapableMtmv;
    private MTMV noIvmMtmv;
    private MTMV autoWithIvmMtmv;

    @BeforeEach
    public void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        catalog = Mockito.mock(InternalCatalog.class);
        db = Mockito.mock(Database.class);
        ctx = Mockito.mock(ConnectContext.class);
        accessManager = Mockito.mock(AccessControllerManager.class);

        // MV with IVM capability (enableIvm = true)
        ivmCapableMtmv = new MTMV();
        ivmCapableMtmv.getIvmInfo().setEnableIvm(true);

        // MV without IVM capability (enableIvm = false, default)
        noIvmMtmv = new MTMV();

        // AUTO MV with IVM capability (RefreshMethod is AUTO but enableIvm = true)
        autoWithIvmMtmv = new MTMV();
        autoWithIvmMtmv.getIvmInfo().setEnableIvm(true);

        // Stub ConnectContext to return a valid NameSpaceContext.
        // TableNameInfo is always fully-qualified (internal, db1, mv1), so only the no-arg
        // NameSpaceContext path is used and ctx.getNameSpaceContext() just needs to return
        // a non-null value.
        Mockito.when(ctx.getNameSpaceContext())
                .thenReturn(new NameSpaceContext("internal", "db1", 0L));

        // Allow privilege check to pass
        Mockito.when(accessManager.checkTblPriv(
                Mockito.any(ConnectContext.class),
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(PrivPredicate.class)))
                .thenReturn(true);

        Mockito.when(env.getAccessManager()).thenReturn(accessManager);

        // Static mocks for Env factory methods
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

        Mockito.when(catalog.getDbOrDdlException(Mockito.anyString())).thenReturn(db);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
    }

    private void setupMvLookup(MTMV mtmv) throws Exception {
        Mockito.when(db.getTableOrMetaException("mv1", TableIf.TableType.MATERIALIZED_VIEW))
                .thenReturn(mtmv);
    }

    private AnalysisException analyzeAndGetException(RefreshMTMVInfo info) {
        try {
            info.analyze(ctx);
            return Assertions.fail("Expected AnalysisException");
        } catch (AnalysisException e) {
            return e;
        }
    }

    private RefreshMTMVInfo createInfo(RefreshMode mode) {
        return new RefreshMTMVInfo(
                new TableNameInfo(ImmutableList.of("internal", "db1", "mv1")),
                ImmutableList.of(),
                mode);
    }

    private RefreshMTMVInfo createInfoWithPartitions(RefreshMode mode) {
        return new RefreshMTMVInfo(
                new TableNameInfo(ImmutableList.of("internal", "db1", "mv1")),
                Lists.newArrayList("p1", "p2"),
                mode);
    }

    // TC-8-5: MV without IVM capability executing REFRESH ... INCREMENTAL should error
    @Test
    public void testRefreshIncrementalOnNonIvmMVRejected() throws Exception {
        setupMvLookup(noIvmMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.INCREMENTAL);
        AnalysisException exception = analyzeAndGetException(info);
        Assertions.assertTrue(exception.getMessage().contains("INCREMENTAL"));
    }

    // TC-8-6: MV without IVM capability executing REFRESH ... PARTITIONS should error
    @Test
    public void testRefreshPartitionsOnNonIvmMVRejected() throws Exception {
        setupMvLookup(noIvmMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.PARTITIONS);
        AnalysisException exception = analyzeAndGetException(info);
        Assertions.assertTrue(exception.getMessage().contains("PARTITIONS"));
    }

    // TC-8-7: IVM-capable MV executing REFRESH ... COMPLETE should succeed
    @Test
    public void testRefreshCompleteOnIvmMVAllowed() throws Exception {
        setupMvLookup(ivmCapableMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.COMPLETE);
        info.analyze(ctx);
    }

    // TC-8-8: IVM-capable MV executing old PARTITION (p1, p2) partitionSpec should error
    @Test
    public void testPartitionSpecOnIvmMVRejected() throws Exception {
        setupMvLookup(ivmCapableMtmv);
        RefreshMTMVInfo info = createInfoWithPartitions(RefreshMode.AUTO);
        AnalysisException exception = analyzeAndGetException(info);
        Assertions.assertTrue(exception.getMessage().contains("partitionSpec"));
        Assertions.assertTrue(exception.getMessage().contains("INCREMENTAL"));
    }

    // IVM-capable MV with REFRESH ... AUTO should succeed
    @Test
    public void testRefreshAutoOnIvmMVAllowed() throws Exception {
        setupMvLookup(ivmCapableMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.AUTO);
        info.analyze(ctx);
    }

    // IVM-capable MV with REFRESH ... INCREMENTAL should succeed
    @Test
    public void testRefreshIncrementalOnIvmMVAllowed() throws Exception {
        setupMvLookup(ivmCapableMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.INCREMENTAL);
        info.analyze(ctx);
    }

    // MV without IVM capability with REFRESH ... COMPLETE should succeed
    @Test
    public void testRefreshCompleteOnNonIvmMVAllowed() throws Exception {
        setupMvLookup(noIvmMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.COMPLETE);
        info.analyze(ctx);
    }

    // AUTO MV with IvmInfo: REFRESH ... INCREMENTAL should succeed (has IVM capability)
    @Test
    public void testRefreshIncrementalOnAutoMVWithIvmAllowed() throws Exception {
        setupMvLookup(autoWithIvmMtmv);
        RefreshMTMVInfo info = createInfo(RefreshMode.INCREMENTAL);
        info.analyze(ctx);
    }

    // AUTO MV with IvmInfo: old partitionSpec should be rejected
    @Test
    public void testPartitionSpecOnAutoMVWithIvmRejected() throws Exception {
        setupMvLookup(autoWithIvmMtmv);
        RefreshMTMVInfo info = createInfoWithPartitions(RefreshMode.AUTO);
        AnalysisException exception = analyzeAndGetException(info);
        Assertions.assertTrue(exception.getMessage().contains("partitionSpec"));
    }
}

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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class IvmRefreshManagerTest {

    @Test
    public void testRefreshContextRejectsNulls(@Mocked MTMV mtmv) {
        ConnectContext connectContext = new ConnectContext();
        org.apache.doris.mtmv.MTMVRefreshContext mtmvRefreshContext = new org.apache.doris.mtmv.MTMVRefreshContext(mtmv);

        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, connectContext, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, null, mtmvRefreshContext));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, connectContext, null));
    }

    @Test
    public void testManagerRejectsNulls() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshManager(null));
    }

    @Test
    public void testManagerReturnsNoBundlesFallback(@Mocked MTMV mtmv) {
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.PLAN_PATTERN_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerExecutesBundles(@Mocked MTMV mtmv, @Mocked Command deltaWriteCommand) {
        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<DeltaCommandBundle> bundles = makeBundles(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), bundles);

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
        Assertions.assertEquals(bundles, executor.lastBundles);
    }

    @Test
    public void testManagerReturnsExecutionFallbackOnExecutorFailure(@Mocked MTMV mtmv,
            @Mocked Command deltaWriteCommand) {
        TestDeltaExecutor executor = new TestDeltaExecutor();
        executor.throwOnExecute = true;
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), makeBundles(deltaWriteCommand, mtmv));

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.INCREMENTAL_EXECUTION_FAILED, result.getFallbackReason());
        Assertions.assertTrue(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails(@Mocked MTMV mtmv) {
        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, null, Collections.emptyList());
        manager.throwOnBuild = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsBinlogBrokenBeforeNereidsFlow(@Mocked MTMV mtmv) {
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setBinlogBroken(true);
        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
            }
        };

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.BINLOG_BROKEN, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerReturnsStreamUnsupportedWithoutBinding(@Mocked MTMV mtmv,
            @Mocked MTMVRelation relation, @Mocked OlapTable olapTable) {
        IvmInfo ivmInfo = new IvmInfo();
        BaseTableInfo baseTableInfo = new BaseTableInfo(olapTable, 2L);
        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
                minTimes = 1;
                mtmv.getRelation();
                result = relation;
                relation.getBaseTablesOneLevelAndFromView();
                result = Sets.newHashSet(baseTableInfo);
            }
        };

        TestDeltaExecutor executor = new TestDeltaExecutor();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor,
                newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(FallbackReason.STREAM_UNSUPPORTED, result.getFallbackReason());
        Assertions.assertFalse(executor.executeCalled);
    }

    @Test
    public void testManagerPassesHealthyPrecheckAndExecutes(@Mocked MTMV mtmv,
            @Mocked MTMVRelation relation, @Mocked OlapTable olapTable, @Mocked Command deltaWriteCommand) {
        IvmInfo ivmInfo = new IvmInfo();
        new Expectations() {
            {
                olapTable.getId();
                result = 1L;
                olapTable.getName();
                result = "t1";
                olapTable.getDBName();
                result = "db1";
            }
        };
        BaseTableInfo baseTableInfo = new BaseTableInfo(olapTable, 2L);
        ivmInfo.setBaseTableStreams(new HashMap<>());
        ivmInfo.getBaseTableStreams().put(baseTableInfo, new IvmStreamRef(StreamType.OLAP, null, null));
        new MockUp<MTMVUtil>() {
            @Mock
            public TableIf getTable(BaseTableInfo input) {
                return olapTable;
            }
        };
        new Expectations() {
            {
                mtmv.getIvmInfo();
                result = ivmInfo;
                minTimes = 1;
                mtmv.getRelation();
                result = relation;
                relation.getBaseTablesOneLevelAndFromView();
                result = Sets.newHashSet(baseTableInfo);
            }
        };

        TestDeltaExecutor executor = new TestDeltaExecutor();
        List<DeltaCommandBundle> bundles = makeBundles(deltaWriteCommand, mtmv);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(executor, newContext(mtmv), bundles);
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(executor.executeCalled);
    }

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext(), new org.apache.doris.mtmv.MTMVRefreshContext(mtmv));
    }

    private static List<DeltaCommandBundle> makeBundles(Command deltaWriteCommand, MTMV mtmv) {
        return Collections.singletonList(new DeltaCommandBundle(deltaWriteCommand));
    }

    private static class TestDeltaExecutor extends IvmDeltaExecutor {
        private boolean executeCalled;
        private boolean throwOnExecute;
        private List<DeltaCommandBundle> lastBundles;

        @Override
        public void execute(IvmRefreshContext context, List<DeltaCommandBundle> bundles) throws AnalysisException {
            executeCalled = true;
            lastBundles = bundles;
            if (throwOnExecute) {
                throw new AnalysisException("executor failed");
            }
        }
    }

    private static class TestIvmRefreshManager extends IvmRefreshManager {
        private final IvmRefreshContext context;
        private final List<DeltaCommandBundle> bundles;
        private boolean throwOnBuild;
        private boolean useSuperPrecheck;

        private TestIvmRefreshManager(IvmDeltaExecutor deltaExecutor,
                IvmRefreshContext context, List<DeltaCommandBundle> bundles) {
            super(deltaExecutor);
            this.context = context;
            this.bundles = bundles;
        }

        @Override
        IvmRefreshResult precheck(MTMV mtmv) {
            if (useSuperPrecheck) {
                return super.precheck(mtmv);
            }
            return IvmRefreshResult.success();
        }

        @Override
        IvmRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
            if (throwOnBuild) {
                throw new AnalysisException("build context failed");
            }
            return context;
        }

        @Override
        List<DeltaCommandBundle> analyzeDeltaCommandBundles(IvmRefreshContext ctx) {
            return bundles;
        }
    }
}

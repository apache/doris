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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccUtil;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class MTMVUtilTest {
    @Test
    public void testExternalTableWithSnapshotIdIsDataChangeAware() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelation relation = Mockito.mock(MTMVRelation.class);
        BaseTableInfo externalTableInfo = Mockito.mock(BaseTableInfo.class);
        TableIf externalTable = Mockito.mock(TableIf.class,
                Mockito.withSettings().extraInterfaces(MTMVRelatedTableIf.class));
        MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) externalTable;
        MvccSnapshot statementSnapshot = Mockito.mock(MvccSnapshot.class);

        Mockito.when(mtmv.getRelation()).thenReturn(relation);
        Mockito.when(relation.getBaseTablesOneLevelAndFromView()).thenReturn(ImmutableSet.of(externalTableInfo));
        Mockito.when(externalTableInfo.isInternalTable()).thenReturn(false);
        Mockito.when(relatedTable.getTableSnapshot(Optional.of(statementSnapshot)))
                .thenReturn(new MTMVSnapshotIdSnapshot(7L));

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(externalTableInfo)).thenReturn(externalTable);
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable))
                    .thenReturn(Optional.of(statementSnapshot));

            Assert.assertFalse(MTMVUtil.mtmvContainsExternalTableWithDataUnawareness(mtmv));
        }
    }

    @Test
    public void testExternalTableWithoutSnapshotIdHasDataUnawareness() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelation relation = Mockito.mock(MTMVRelation.class);
        BaseTableInfo externalTableInfo = Mockito.mock(BaseTableInfo.class);
        TableIf externalTable = Mockito.mock(TableIf.class,
                Mockito.withSettings().extraInterfaces(MTMVRelatedTableIf.class));
        MTMVRelatedTableIf relatedTable = (MTMVRelatedTableIf) externalTable;
        MvccSnapshot statementSnapshot = Mockito.mock(MvccSnapshot.class);

        Mockito.when(mtmv.getRelation()).thenReturn(relation);
        Mockito.when(relation.getBaseTablesOneLevelAndFromView()).thenReturn(ImmutableSet.of(externalTableInfo));
        Mockito.when(externalTableInfo.isInternalTable()).thenReturn(false);
        Mockito.when(relatedTable.getTableSnapshot(Optional.of(statementSnapshot)))
                .thenReturn(new MTMVTimestampSnapshot(7L));

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(externalTableInfo)).thenReturn(externalTable);
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable))
                    .thenReturn(Optional.of(statementSnapshot));

            Assert.assertTrue(MTMVUtil.mtmvContainsExternalTableWithDataUnawareness(mtmv));
        }
    }

    @Test
    public void testExternalTableWithoutStatementSnapshotHasDataUnawareness() throws AnalysisException {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRelation relation = Mockito.mock(MTMVRelation.class);
        BaseTableInfo externalTableInfo = Mockito.mock(BaseTableInfo.class);
        TableIf externalTable = Mockito.mock(TableIf.class,
                Mockito.withSettings().extraInterfaces(MTMVRelatedTableIf.class));

        Mockito.when(mtmv.getRelation()).thenReturn(relation);
        Mockito.when(relation.getBaseTablesOneLevelAndFromView()).thenReturn(ImmutableSet.of(externalTableInfo));
        Mockito.when(externalTableInfo.isInternalTable()).thenReturn(false);

        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<MvccUtil> mvccUtil = Mockito.mockStatic(MvccUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getTable(externalTableInfo)).thenReturn(externalTable);
            mvccUtil.when(() -> MvccUtil.getSnapshotFromContext(externalTable)).thenReturn(Optional.empty());

            Assert.assertTrue(MTMVUtil.mtmvContainsExternalTableWithDataUnawareness(mtmv));
        }
    }

    @Test
    public void testGetExprTimeSec() throws AnalysisException {
        LiteralExpr expr = new DateLiteral("2020-01-01");
        long exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.empty());
        Assert.assertEquals(1577808000L, exprTimeSec);

        expr = new StringLiteral("2020-01-01");
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.of("%Y-%m-%d"));
        Assert.assertEquals(1577808000L, exprTimeSec);

        expr = new IntLiteral(20200101);
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.of("%Y%m%d"));
        Assert.assertEquals(1577808000L, exprTimeSec);

        expr = new DateLiteral(Type.DATE, true);
        exprTimeSec = MTMVUtil.getExprTimeSec(expr, Optional.empty());
        Assert.assertEquals(253402185600L, exprTimeSec);
    }
}

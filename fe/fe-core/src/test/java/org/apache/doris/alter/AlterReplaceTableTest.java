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

import org.apache.doris.catalog.CatalogRecycleBin;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.constraint.DistributionMappingConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.statistics.AnalysisManager;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

class AlterReplaceTableTest {

    @Test
    void mappingTableReplacementKeepsFrontendAdmissionFencedUntilRecycle() throws Exception {
        Alter alter = new Alter();
        Env env = Mockito.mock(Env.class);
        Database db = Mockito.mock(Database.class);
        OlapTable originalTable = Mockito.mock(OlapTable.class);
        OlapTable replacementTable = Mockito.mock(OlapTable.class);
        ConstraintManager constraintManager = Mockito.mock(ConstraintManager.class);
        CatalogRecycleBin recycleBin = Mockito.mock(CatalogRecycleBin.class);
        AnalysisManager analysisManager = Mockito.mock(AnalysisManager.class);
        DistributionMappingConstraint mapping = new DistributionMappingConstraint(
                "mapping", "mapping_id", List.of("d1"), List.of("k1"));

        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(originalTable.getId()).thenReturn(2L);
        Mockito.when(originalTable.getName()).thenReturn("original");
        Mockito.when(replacementTable.getName()).thenReturn("replacement");
        Mockito.when(env.getConstraintManager()).thenReturn(constraintManager);
        Mockito.when(env.getAnalysisManager()).thenReturn(analysisManager);
        Mockito.when(constraintManager.getDistributionMappingConstraints(originalTable))
                .thenReturn(ImmutableList.of(mapping));
        Mockito.when(constraintManager.acquireFrontendAdmissionFence()).thenReturn(true);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentRecycleBin).thenReturn(recycleBin);
            Deencapsulation.invoke(alter, "replaceTableInternal",
                    db, originalTable, replacementTable, false, false, false);
        }

        TableNameInfo originalTableInfo = new TableNameInfo("internal", "db1", "original");
        TableNameInfo replacementTableInfo = new TableNameInfo("internal", "db1", "replacement");
        InOrder order = Mockito.inOrder(constraintManager, recycleBin);
        order.verify(constraintManager).acquireFrontendAdmissionFence();
        order.verify(constraintManager).dropAndRenameConstraints(
                originalTableInfo, replacementTableInfo);
        order.verify(recycleBin).recycleTable(1L, originalTable, false, false, 0);
        order.verify(constraintManager).releaseFrontendAdmissionFence();
    }
}

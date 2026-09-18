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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;

public class CollectRelationTest {

    @Test
    public void testCollectMTMVCandidatesSkipsTableWithoutDatabase() throws Exception {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("tbl_without_owner");
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.getPlannerHooks()).thenReturn(
                Collections.singleton(InitMaterializationContextHook.INSTANCE));

        Method collectMethod = CollectRelation.class.getDeclaredMethod(
                "collectMTMVCandidates", TableIf.class, CascadesContext.class);
        collectMethod.setAccessible(true);

        Assertions.assertDoesNotThrow(
                () -> collectMethod.invoke(new CollectRelation(false), table, cascadesContext));
    }
}

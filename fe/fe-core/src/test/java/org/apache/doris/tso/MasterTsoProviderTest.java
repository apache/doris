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

package org.apache.doris.tso;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MasterTsoProviderTest {
    @Test
    void testMasterReturnsCurrentTso() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        Mockito.when(tsoService.getTSO()).thenReturn(123L);

        Assertions.assertEquals(123L, MasterTsoProvider.getCurrentTso(context));
    }

    @Test
    void testMasterRejectsNonPositiveTso() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        Mockito.when(tsoService.getTSO()).thenReturn(0L);

        Assertions.assertThrows(AnalysisException.class,
                () -> MasterTsoProvider.getCurrentTso(context));
    }
}

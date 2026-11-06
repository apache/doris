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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MTMVCacheContextLifecycleTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void parseFailureRestoresCallerAndSessionFlags(boolean hasParent) {
        boolean savedUnitTest = FeConstants.runningUnitTest;
        ConnectContext savedContext = ConnectContext.get();
        FeConstants.runningUnitTest = true;
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
            ConnectContext parent = hasParent ? new ConnectContext() : null;
            ConnectContext cacheContext = new ConnectContext();
            cacheContext.getSessionVariable().enableMaterializedViewRewrite = true;
            cacheContext.setThreadLocalInfo();
            try {
                Assertions.assertThrows(ParseException.class, () -> MTMVCache.from("select from",
                        cacheContext, false, false, parent, false));
                Assertions.assertSame(parent, ConnectContext.get());
                Assertions.assertTrue(cacheContext.getSessionVariable().enableMaterializedViewRewrite);
                if (parent != null) {
                    Assertions.assertFalse(parent.isClosingOrClosed());
                }
            } finally {
                cacheContext.cleanup();
                if (parent != null) {
                    parent.cleanup();
                }
            }
        } finally {
            FeConstants.runningUnitTest = savedUnitTest;
            if (savedContext == null) {
                ConnectContext.remove();
            } else {
                savedContext.setThreadLocalInfo();
            }
        }
    }
}

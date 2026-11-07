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

package org.apache.doris.catalog;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class MaterializedIndexMetaContextTest {
    @Test
    void indexWithoutDefinitionDoesNotCreateSessionForStoredVariables() throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        MaterializedIndexMeta index = new MaterializedIndexMeta(1,
                List.of(new Column("k", Type.INT)), 1, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(), "db",
                Map.of(SessionVariable.TIME_ZONE, "UTC"));
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            index.parseStmt();
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertNull(ConnectContext.get());
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}

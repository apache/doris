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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

public class VectorSearchTableValuedFunctionTest {
    private static final UserIdentity USER = UserIdentity.createAnalyzedUserIdentWithIp("reader", "%");
    private static final Set<String> COLUMNS = Collections.singleton("embedding");

    @Test
    public void sourceWithoutPoliciesIsAllowed() {
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Assertions.assertDoesNotThrow(() -> check(access, USER));
        Mockito.verify(access).evalRowFilterPolicies(USER, "catalog", "database", "table");
        Mockito.verify(access).evalDataMaskPolicies(USER, "catalog", "database", "table", COLUMNS);
    }

    @Test
    public void sourceRowFilterIsRejectedBeforeCandidateGeneration() {
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(access.evalRowFilterPolicies(USER, "catalog", "database", "table"))
                .thenReturn(Collections.singletonList(RowFilterSpec.restrictive("policy", "id > 0")));
        Assertions.assertThrows(AnalysisException.class, () -> check(access, USER));
    }

    @Test
    public void sourceColumnMaskIsRejectedBeforeMaterialization() {
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(access.evalDataMaskPolicies(USER, "catalog", "database", "table", COLUMNS))
                .thenReturn(Collections.singletonMap("embedding", new DataMaskSpec("mask", "NULL")));
        Assertions.assertThrows(AnalysisException.class, () -> check(access, USER));
    }

    @Test
    public void systemAccountKeepsTheOrdinaryScanPolicyExemption() {
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Assertions.assertDoesNotThrow(() -> check(access, UserIdentity.ROOT));
        Mockito.verifyNoInteractions(access);
    }

    private static void check(AccessControllerManager access, UserIdentity user) {
        VectorSearchTableValuedFunction.rejectSourceDataPolicies(
                access, user, "catalog", "database", "table", COLUMNS);
    }
}

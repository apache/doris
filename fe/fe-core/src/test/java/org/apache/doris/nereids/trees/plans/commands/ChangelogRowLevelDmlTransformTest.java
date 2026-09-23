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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Collections;

public class ChangelogRowLevelDmlTransformTest {

    @Test
    public void rejectsMaskedTargetForNonAdminUser() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        UserIdentity user = Mockito.mock(UserIdentity.class);
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);

        Mockito.when(context.getCurrentUserIdentity()).thenReturn(user);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(database.getFullName()).thenReturn("database");
        Mockito.when(table.getName()).thenReturn("target");
        Mockito.when(table.getFullSchema()).thenReturn(
                ImmutableList.of(new Column("id", ScalarType.INT)));
        Mockito.when(accessManager.evalDataMaskPolicies(
                Mockito.eq(user), Mockito.eq("catalog"), Mockito.eq("database"), Mockito.eq("target"),
                ArgumentMatchers.anySet())).thenReturn(
                ImmutableMap.of("id", new DataMaskSpec("masked", "mask(id)")));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ChangelogRowLevelDmlTransform.requireNoDataMask(
                        context, table, RowLevelDmlOp.UPDATE));
        Assertions.assertTrue(exception.getMessage().contains("data masking policies"));
    }

    @Test
    public void allowsUnmaskedTargetAndTrustedUsers() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        UserIdentity user = Mockito.mock(UserIdentity.class);
        Env env = Mockito.mock(Env.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        ExternalDatabase<?> database = Mockito.mock(ExternalDatabase.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);

        Mockito.when(context.getCurrentUserIdentity()).thenReturn(user);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        Mockito.when(database.getFullName()).thenReturn("database");
        Mockito.when(table.getName()).thenReturn("target");
        Mockito.when(table.getFullSchema()).thenReturn(
                ImmutableList.of(new Column("id", ScalarType.INT)));
        Mockito.when(accessManager.evalDataMaskPolicies(
                Mockito.eq(user), Mockito.eq("catalog"), Mockito.eq("database"), Mockito.eq("target"),
                ArgumentMatchers.anySet())).thenReturn(Collections.emptyMap());

        Assertions.assertDoesNotThrow(() -> ChangelogRowLevelDmlTransform.requireNoDataMask(
                context, table, RowLevelDmlOp.DELETE));

        Mockito.when(user.isRootUser()).thenReturn(true);
        Assertions.assertDoesNotThrow(() -> ChangelogRowLevelDmlTransform.requireNoDataMask(
                context, table, RowLevelDmlOp.MERGE));
    }
}

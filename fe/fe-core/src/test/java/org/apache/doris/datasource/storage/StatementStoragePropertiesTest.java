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

package org.apache.doris.datasource.storage;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

class StatementStoragePropertiesTest {
    @ParameterizedTest
    @CsvSource({
            "dfs.replication, 3",
            "broker.DFS.NAMESERVICES, cluster",
            "fs.hdfs.impl.disable.cache, true",
            "juicefs.volume.meta, redis://metadata",
            "hdfs.authentication.type, simple",
            "hdfs.impersonation.enabled, true",
            "ipc.client.fallback-to-simple-auth-allowed, true",
            "fs.viewfs.mounttable.cluster.link./warehouse, hdfs://nn/warehouse",
            "fs.viewfs.mounttable.cluster.homedir, /warehouse"
    })
    void requiresGlobalAdmin(String key, String value) {
        Map<String, String> properties = Map.of(key, value);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(ctx.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        Mockito.when(ctx.getEnv()).thenReturn(env);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        StatementContext statement = new StatementContext(ctx, null);
        Mockito.when(ctx.getStatementContext()).thenReturn(statement);
        try (MockedStatic<ConnectContext> current = Mockito.mockStatic(ConnectContext.class)) {
            current.when(ConnectContext::get).thenReturn(ctx);
            Mockito.when(access.checkGlobalPriv(ctx, PrivPredicate.ADMIN)).thenReturn(false, true, false);
            AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                    () -> StatementStorageProperties.check(properties));
            Assertions.assertEquals("Storage property requires ADMIN_PRIV: " + key, denied.getMessage());
            StatementStorageProperties.check(properties);
            StatementContext prepared = statement.createNextExecuteContext();
            Assertions.assertEquals(key, prepared.getAdminStorageProperty());
            Assertions.assertThrows(AnalysisException.class,
                    () -> StatementStorageProperties.checkAdmin(ctx, prepared.getAdminStorageProperty()));
        }
    }

    @Test
    void requiresAuthenticatedSessionForCustomProperties() {
        try (MockedStatic<ConnectContext> ignored = Mockito.mockStatic(ConnectContext.class)) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> StatementStorageProperties.check(Map.of("dfs.replication", "1")));
        }
    }

    @ParameterizedTest
    @CsvSource({
            "fs.defaultFS, hdfs:///",
            "fs.defaultFS, file:///",
            "oss.hdfs.fs.defaultFS, relative",
            "fs.viewfs.mounttable.cluster.link./warehouse, relative",
            "fs.hdfs.support, true",
            "fs.hdfs.impl, test.Provider",
            "hadoop.config.resources, fixture.xml",
            "oss.hdfs.hadoop.config.resources, fixture.xml",
            "_HADOOP_CONFIG_DIR_, fixture"
    })
    void enforcesStorageRulesBeforeCheckingPrivileges(String key, String value) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> StatementStorageProperties.check(Map.of(key, value)));
        Assertions.assertTrue(error.getMessage().endsWith(": " + key));
        Assertions.assertFalse(error.getMessage().contains("ADMIN_PRIV"));
    }

    @ParameterizedTest
    @CsvSource({
            "fs.defaultFS, hdfs://nameservice",
            "oss.hdfs.fs.defaultFS, oss://bucket",
            "broker.hadoop.username, doris",
            "format, parquet",
            "s3.endpoint, s3.example.org"
    })
    void acceptsStatementOptions(String key, String value) {
        Assertions.assertDoesNotThrow(() -> StatementStorageProperties.check(Map.of(key, value)));
    }
}

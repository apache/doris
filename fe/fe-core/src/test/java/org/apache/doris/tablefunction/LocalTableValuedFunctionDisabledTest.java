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

import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class LocalTableValuedFunctionDisabledTest {
    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "file_path", "local_disabled_test.csv", "backend_id", "1", "format", "csv");

    private final Env env = Mockito.mock(Env.class);
    private final ConnectContext context = Mockito.mock(ConnectContext.class);
    private final AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private final SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
    private final BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedContext;
    private MockedStatic<BackendServiceProxy> mockedProxy;

    @BeforeEach
    public void setUp() {
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        mockedContext = Mockito.mockStatic(ConnectContext.class);
        mockedContext.when(ConnectContext::get).thenReturn(context);
        mockedProxy = Mockito.mockStatic(BackendServiceProxy.class);
        mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
    }

    @AfterEach
    public void tearDown() {
        mockedProxy.close();
        mockedContext.close();
        mockedEnv.close();
    }

    @Test
    public void testLocalDisabledBeforePropertyParsing() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new LocalTableValuedFunction(ImmutableMap.of()));
        Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
        Mockito.verifyNoInteractions(accessManager, systemInfo, proxy);
    }

    @Test
    public void testFormatsDisabledBeforeFileAccess() {
        for (String format : new String[] {"csv", "json", "lance"}) {
            Map<String, String> properties = ImmutableMap.of(
                    "file_path", "local_disabled_test", "backend_id", "1", "format", format);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> new LocalTableValuedFunction(properties));
            Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
        }
        Mockito.verifyNoInteractions(accessManager, systemInfo, proxy);
    }

    @Test
    public void testFileLocalDisabledBeforeFileAccess() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new FileTableValuedFunction(PROPERTIES));
        Assertions.assertTrue(exception.getMessage().contains("LOCAL TVF is disabled"));
        Mockito.verifyNoInteractions(accessManager, systemInfo, proxy);
    }

    @Test
    public void testPrivilegedUsersCannotAccessLocalFiles() {
        for (PrivPredicate privilege : new PrivPredicate[] {PrivPredicate.ADMIN, PrivPredicate.OPERATOR}) {
            Mockito.when(accessManager.checkGlobalPriv(context, privilege)).thenReturn(true);
            Assertions.assertThrows(AnalysisException.class, () -> new LocalTableValuedFunction(PROPERTIES));
            Assertions.assertThrows(AnalysisException.class, () -> new FileTableValuedFunction(PROPERTIES));
        }
        Mockito.verifyNoInteractions(accessManager, systemInfo, proxy);
    }

    @Test
    public void testFileS3RemainsAvailable() {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("uri", "s3://test-bucket/local_disabled_test.csv")
                .put("s3.endpoint", "s3.us-east-1.amazonaws.com")
                .put("s3.region", "us-east-1")
                .put("s3.access_key", "test-access-key")
                .put("s3.secret_key", "test-secret-key")
                .put("format", "csv")
                .build();
        try (MockedConstruction<S3TableValuedFunction> mockedS3 =
                     Mockito.mockConstruction(S3TableValuedFunction.class)) {
            Assertions.assertDoesNotThrow(() -> new FileTableValuedFunction(properties));
            Assertions.assertEquals(1, mockedS3.constructed().size());
        }
        Mockito.verifyNoInteractions(accessManager, systemInfo, proxy);
    }
}

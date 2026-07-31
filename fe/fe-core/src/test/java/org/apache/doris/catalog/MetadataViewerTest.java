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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Replica.ReplicaStatus;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class MetadataViewerTest {

    private static Method getTabletStatusMethod;
    private static Method getTabletDistributionMethod;

    private Env env = Mockito.mock(Env.class);

    private InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);

    private SystemInfoService infoService = Mockito.mock(SystemInfoService.class);

    private MockedStatic<Env> mockedEnvStatic;

    private static Database db;

    @BeforeClass
    public static void setUp() throws Exception {
        Class[] argTypes = new Class[] {String.class, String.class, List.class, ReplicaStatus.class, Operator.class};
        getTabletStatusMethod = MetadataViewer.class.getDeclaredMethod("getTabletStatus", argTypes);
        getTabletStatusMethod.setAccessible(true);

        argTypes = new Class[] { String.class, String.class, PartitionNamesInfo.class };
        getTabletDistributionMethod = MetadataViewer.class.getDeclaredMethod("getTabletDistribution", argTypes);
        getTabletDistributionMethod.setAccessible(true);

        db = CatalogMocker.mockDb();
    }

    @Before
    public void before() throws Exception {
        mockedEnvStatic = Mockito.mockStatic(Env.class);

        Mockito.when(internalCatalog.getDbOrDdlException(Mockito.anyString())).thenReturn(db);

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);

        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);
        Mockito.when(infoService.getAllBackendIds(Mockito.anyBoolean()))
                .thenReturn(Lists.newArrayList(10000L, 10001L, 10002L));
    }

    @After
    public void after() {
        mockedEnvStatic.close();
    }

    @Test
    public void testGetTabletStatus()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<String> partitions = Lists.newArrayList();
        Object[] args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, null,
                null };
        List<List<String>> result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());

        args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.EQ };
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());

        args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.NE };
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testGetTabletDistribution()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Object[] args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, null };
        List<List<String>> result = (List<List<String>>) getTabletDistributionMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());
    }

}

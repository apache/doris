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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.metric.AutoMappedMetric;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TUniqueId;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class QeProcessorImplTest {
    private static final QeProcessorImpl QE_PROCESSOR = (QeProcessorImpl) QeProcessorImpl.INSTANCE;

    private boolean originalMetricInit;
    private AutoMappedMetric<LongCounterMetric> originalQueryInstanceMetric;

    @Before
    public void setUp() throws Exception {
        clearQeProcessorState();
        originalMetricInit = MetricRepo.isInit;
        originalQueryInstanceMetric = MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN;
    }

    @After
    public void tearDown() throws Exception {
        MetricRepo.isInit = originalMetricInit;
        MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN = originalQueryInstanceMetric;
        clearQeProcessorState();
    }

    @Test
    public void testRegisterInstancesSkipsMetricBeforeMetricRepoInit() throws Exception {
        String user = "cir_20036_metric_not_init";
        TUniqueId queryId = new TUniqueId(1L, 2L);
        registerQuery(queryId, user);

        MetricRepo.isInit = false;
        MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN = null;

        QE_PROCESSOR.registerInstances(queryId, 3);

        Assert.assertEquals(Integer.valueOf(3), QE_PROCESSOR.getInstancesNumPerUser().get(user));
        Assert.assertEquals(Integer.valueOf(3), getQueryToInstancesNum().get(queryId));
    }

    @Test
    public void testRegisterInstancesUpdatesMetricAfterMetricRepoInit() throws Exception {
        String user = "cir_20036_metric_ready";
        TUniqueId queryId = new TUniqueId(3L, 4L);
        registerQuery(queryId, user);

        MetricRepo.isInit = true;
        MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN = new AutoMappedMetric<>(
                ignored -> new LongCounterMetric("query_instance_begin", MetricUnit.NOUNIT, "test metric"));

        QE_PROCESSOR.registerInstances(queryId, 2);

        Assert.assertEquals(Integer.valueOf(2), QE_PROCESSOR.getInstancesNumPerUser().get(user));
        Assert.assertEquals(Integer.valueOf(2), getQueryToInstancesNum().get(queryId));
        Assert.assertEquals(Long.valueOf(2L),
                MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN.getOrAdd(user).getValue());
    }

    private void registerQuery(TUniqueId queryId, String user) throws Exception {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        Auth auth = Mockito.mock(Auth.class);
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        TQueryOptions queryOptions = new TQueryOptions();

        Mockito.when(connectContext.getQualifiedUser()).thenReturn(user);
        Mockito.when(connectContext.getEnv()).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(auth.getMaxQueryInstances(user)).thenReturn(Long.MAX_VALUE);
        Mockito.when(coordinator.getQueryOptions()).thenReturn(queryOptions);

        QE_PROCESSOR.registerQuery(queryId, new QeProcessorImpl.QueryInfo(connectContext, "select 1", coordinator));
    }

    private void clearQeProcessorState() throws Exception {
        getCoordinatorMap().clear();
        getQueryToInstancesNum().clear();
        getUserToInstancesCount().clear();
    }

    @SuppressWarnings("unchecked")
    private Map<TUniqueId, QeProcessorImpl.QueryInfo> getCoordinatorMap() throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("coordinatorMap");
        field.setAccessible(true);
        return (Map<TUniqueId, QeProcessorImpl.QueryInfo>) field.get(QE_PROCESSOR);
    }

    @SuppressWarnings("unchecked")
    private Map<TUniqueId, Integer> getQueryToInstancesNum() throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("queryToInstancesNum");
        field.setAccessible(true);
        return (Map<TUniqueId, Integer>) field.get(QE_PROCESSOR);
    }

    @SuppressWarnings("unchecked")
    private Map<String, AtomicInteger> getUserToInstancesCount() throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("userToInstancesCount");
        field.setAccessible(true);
        return (Map<String, AtomicInteger>) field.get(QE_PROCESSOR);
    }
}

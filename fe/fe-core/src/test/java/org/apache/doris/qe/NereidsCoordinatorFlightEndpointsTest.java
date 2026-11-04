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

import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.service.arrowflight.results.FlightSqlEndpointsLocation;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class NereidsCoordinatorFlightEndpointsTest {
    private static final TUniqueId QUERY_ID = new TUniqueId(1, 2);

    @Test
    public void parallelInstancesShareOneEndpointPerBackend() throws Exception {
        List<FlightSqlEndpointsLocation> endpoints = endpoints(true, false, 1, 8);
        Assert.assertEquals(1, endpoints.size());
        Assert.assertEquals(QUERY_ID, endpoints.get(0).getFinstId());
    }

    @Test
    public void parallelEndpointsRetainAllResultBackendsInOrder() throws Exception {
        List<FlightSqlEndpointsLocation> endpoints = endpoints(true, false, 6, 8);
        Assert.assertEquals(6, endpoints.size());
        for (int i = 0; i < endpoints.size(); i++) {
            Assert.assertEquals(QUERY_ID, endpoints.get(i).getFinstId());
            Assert.assertEquals(new TNetworkAddress("127.0.0.1", 8100 + i),
                    endpoints.get(i).getResultInternalServiceAddr());
        }
    }

    @Test
    public void sharedFlightAddressDoesNotMergeDifferentBackends() throws Exception {
        List<FlightSqlEndpointsLocation> endpoints = endpoints(true, false, 2, 2);
        Assert.assertEquals(2, endpoints.size());
        Assert.assertEquals(endpoints.get(0).getResultFlightServerAddr(),
                endpoints.get(1).getResultFlightServerAddr());
        Assert.assertNotEquals(endpoints.get(0).getResultInternalServiceAddr(),
                endpoints.get(1).getResultInternalServiceAddr());
    }

    @Test
    public void nonParallelEndpointsKeepDistinctInstanceIds() throws Exception {
        List<FlightSqlEndpointsLocation> endpoints = endpoints(false, false, 1, 2);
        Assert.assertEquals(2, endpoints.size());
        Assert.assertEquals(new TUniqueId(2, 0), endpoints.get(0).getFinstId());
        Assert.assertEquals(new TUniqueId(2, 1), endpoints.get(1).getFinstId());
    }

    @Test
    public void localResultDoesNotPublishFlightEndpoints() throws Exception {
        Assert.assertTrue(endpoints(true, true, 2, 8).isEmpty());
    }

    private List<FlightSqlEndpointsLocation> endpoints(boolean parallel, boolean local,
            int backendCount, int instancesPerBackend) throws Exception {
        ConnectContext context = new ConnectContext();
        context.connectType = ConnectContext.ConnectType.ARROW_FLIGHT_SQL;
        context.setReturnResultFromLocal(local);
        context.getSessionVariable().setEnableParallelResultSink(parallel);
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        setContextField(coordinatorContext, "connectContext", context);
        setContextField(coordinatorContext, "dataSink", Mockito.mock(ResultSink.class));
        PipelineDistributedPlan plan = Mockito.mock(PipelineDistributedPlan.class, Mockito.RETURNS_DEEP_STUBS);
        List<AssignedJob> jobs = new ArrayList<>();
        for (int i = 0; i < backendCount; i++) {
            Backend backend = new Backend(i + 1, "127.0.0.1", 9000 + i);
            backend.setBrpcPort(8100 + i);
            // Backend identity must remain distinct even when Flight locations are shared.
            backend.setArrowFlightSqlPort(8050);
            for (int j = 0; j < instancesPerBackend; j++) {
                AssignedJob job = Mockito.mock(AssignedJob.class);
                Mockito.when(job.getAssignedWorker()).thenReturn(new BackendWorker(0, backend));
                Mockito.when(job.instanceId()).thenReturn(new TUniqueId(2, i * instancesPerBackend + j));
                jobs.add(job);
            }
        }
        Mockito.when(plan.getInstanceJobs()).thenReturn(ImmutableList.copyOf(jobs));
        Mockito.when(plan.getFragmentJob().getFragment().getOutputExprs()).thenReturn(new ArrayList<>());
        NereidsCoordinator coordinator = Mockito.mock(NereidsCoordinator.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(QUERY_ID).when(coordinator).getQueryId();
        coordinator.processTopSink(coordinatorContext, plan);
        return context.getFlightSqlEndpointsLocations();
    }

    private void setContextField(CoordinatorContext context, String name, Object value) throws Exception {
        Field field = CoordinatorContext.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(context, value);
    }
}

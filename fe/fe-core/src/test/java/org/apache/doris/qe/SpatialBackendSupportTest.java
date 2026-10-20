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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.qe.runtime.PipelineExecutionTaskBuilder;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TSlotDescriptor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class SpatialBackendSupportTest {
    @Test
    void testSpatialScanSlotAndNestedTypeRequireAllWorkers() {
        for (Type type : Arrays.asList(ScalarType.createGeometryType(), ScalarType.createGeographyType(),
                ArrayType.create(ScalarType.createGeometryType(), true))) {
            TDescriptorTable descriptors = new TDescriptorTable();
            descriptors.addToSlotDescriptors(new TSlotDescriptor().setSlotType(type.toThrift()));
            TPipelineFragmentParams params = new TPipelineFragmentParams().setDescTbl(descriptors);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> SpatialBackendSupport.check(Collections.singletonList(params),
                            Arrays.asList(backend(1, true), backend(2, false))));
            Assertions.assertTrue(error.getMessage().contains("Backend 2"));
            Assertions.assertDoesNotThrow(() -> SpatialBackendSupport.check(Collections.singletonList(params),
                    Collections.singletonList(backend(1, true))));
        }
    }

    @Test
    void testConstructorUnderBinaryResultRejectedBeforeBuildingAnyRpc() {
        TExpr expression = new TExpr();
        expression.addToNodes(new TExprNode().setType(Type.VARBINARY.toThrift()));
        expression.addToNodes(new TExprNode().setType(ScalarType.createGeographyType().toThrift()));
        TPlanFragment fragment = new TPlanFragment().setOutputExprs(Collections.singletonList(expression));
        TPipelineFragmentParamsList params = new TPipelineFragmentParamsList()
                .setParamsList(Collections.singletonList(new TPipelineFragmentParams().setFragment(fragment)));
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> workers = new LinkedHashMap<>();
        workers.put(new BackendWorker(0, backend(1, true)), params);
        workers.put(new BackendWorker(0, backend(2, false)), new TPipelineFragmentParamsList());
        try (MockedStatic<BackendServiceProxy> proxy = Mockito.mockStatic(BackendServiceProxy.class)) {
            Assertions.assertThrows(AnalysisException.class, () -> PipelineExecutionTaskBuilder.build(null, workers));
            proxy.verifyNoInteractions();
        }
    }

    @Test
    void testOrdinaryPlansDoNotRequireUpgrade() {
        TDescriptorTable descriptors = new TDescriptorTable();
        descriptors.addToSlotDescriptors(new TSlotDescriptor().setSlotType(Type.INT.toThrift()));
        Assertions.assertDoesNotThrow(() -> SpatialBackendSupport.check(
                Collections.singletonList(new TPipelineFragmentParams().setDescTbl(descriptors)),
                Collections.singletonList(backend(1, false))));
    }

    private Backend backend(long id, boolean supported) {
        Backend backend = new Backend(id, "127.0.0.1", 9050);
        BackendHbResponse heartbeat = new BackendHbResponse(id, 9060, 8040, 8060, 1000, 1000,
                "same-execution-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        heartbeat.setSpatialTypesSupported(supported);
        backend.handleHbResponse(heartbeat, false);
        return backend;
    }
}

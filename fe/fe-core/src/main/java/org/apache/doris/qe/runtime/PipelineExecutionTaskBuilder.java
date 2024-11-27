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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.protocol.TFastSerializer;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.thrift.protocol.TCompactProtocol.Factory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PipelineExecutionTaskBuilder {
    private CoordinatorContext coordinatorContext;

    public PipelineExecutionTaskBuilder(CoordinatorContext coordinatorContext) {
        this.coordinatorContext = coordinatorContext;
    }

    public static PipelineExecutionTask build(CoordinatorContext coordinatorContext,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {
        PipelineExecutionTaskBuilder builder = new PipelineExecutionTaskBuilder(coordinatorContext);
        return builder.buildTask(coordinatorContext, workerToFragmentsParam);
    }

    private PipelineExecutionTask buildTask(CoordinatorContext coordinatorContext,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {
        BackendServiceProxy backendServiceProxy = BackendServiceProxy.getInstance();
        PipelineExecutionTask pipelineExecutionTask = new PipelineExecutionTask(
                coordinatorContext,
                backendServiceProxy,
                buildMultiFragmentTasks(coordinatorContext, backendServiceProxy, workerToFragmentsParam)
        );
        coordinatorContext.getJobProcessor().setSqlPipelineTask(pipelineExecutionTask);
        return pipelineExecutionTask;
    }

    private Map<Long, MultiFragmentsPipelineTask> buildMultiFragmentTasks(
            CoordinatorContext coordinatorContext, BackendServiceProxy backendServiceProxy,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {

        Map<DistributedPlanWorker, ByteString> workerToSerializeFragments = serializeFragments(workerToFragmentsParam);

        Map<Long, MultiFragmentsPipelineTask> fragmentTasks = Maps.newLinkedHashMap();
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv :
                workerToFragmentsParam.entrySet()) {
            BackendWorker worker = (BackendWorker) kv.getKey();
            TPipelineFragmentParamsList fragmentParamsList = kv.getValue();
            ByteString serializeFragments = workerToSerializeFragments.get(worker);

            Backend backend = worker.getBackend();
            fragmentTasks.put(
                    worker.id(),
                    new MultiFragmentsPipelineTask(
                            coordinatorContext,
                            backend,
                            backendServiceProxy,
                            serializeFragments,
                            buildSingleFragmentPipelineTask(backend, fragmentParamsList)
                    )
            );
        }
        return fragmentTasks;
    }

    private Map<Integer, SingleFragmentPipelineTask> buildSingleFragmentPipelineTask(
            Backend backend, TPipelineFragmentParamsList fragmentParamsList) {
        Map<Integer, SingleFragmentPipelineTask> tasks = Maps.newLinkedHashMap();
        for (TPipelineFragmentParams fragmentParams : fragmentParamsList.getParamsList()) {
            int fragmentId = fragmentParams.getFragmentId();
            Set<TUniqueId> instanceIds = fragmentParams.getLocalParams()
                    .stream()
                    .map(TPipelineInstanceParams::getFragmentInstanceId)
                    .collect(Collectors.toSet());
            tasks.put(fragmentId, new SingleFragmentPipelineTask(backend, fragmentId, instanceIds));
        }
        return tasks;
    }

    private Map<DistributedPlanWorker, ByteString> serializeFragments(
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {

        AtomicLong compressedSize = new AtomicLong(0);
        Map<DistributedPlanWorker, ByteString> serializedFragments = workerToFragmentsParam.entrySet()
                .parallelStream()
                .map(kv -> {
                    try {
                        ByteString serializeString =
                                new TFastSerializer(1024, new Factory()).serialize(kv.getValue());
                        return Pair.of(kv.getKey(), serializeString);
                    } catch (Throwable t) {
                        throw new IllegalStateException(t.getMessage(), t);
                    }
                })
                .peek(kv -> compressedSize.addAndGet(kv.second.size()))
                .collect(Collectors.toMap(Pair::key, Pair::value));

        coordinatorContext.updateProfileIfPresent(
                profile -> profile.updateFragmentCompressedSize(compressedSize.get())
        );
        coordinatorContext.updateProfileIfPresent(SummaryProfile::setFragmentSerializeTime);

        return serializedFragments;
    }
}

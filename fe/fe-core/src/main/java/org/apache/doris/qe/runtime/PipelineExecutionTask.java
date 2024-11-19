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

import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * SqlPipelineTask.
 *
 * This class is used to describe which backend process which fragments
 */
public class PipelineExecutionTask extends AbstractRuntimeTask<Long, MultiFragmentsPipelineTask> {
    private static final Logger LOG = LogManager.getLogger(PipelineExecutionTask.class);

    // immutable parameters
    private final long timeoutDeadline;
    private final CoordinatorContext coordinatorContext;
    private final BackendServiceProxy backendServiceProxy;

    // mutable states
    public PipelineExecutionTask(
            CoordinatorContext coordinatorContext,
            BackendServiceProxy backendServiceProxy,
            Map<Long, MultiFragmentsPipelineTask> fragmentTasks) {
        // insert into stmt need latch to wait finish, but query stmt not need because result receiver can wait finish
        super(new ChildrenRuntimeTasks<>(fragmentTasks));
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.backendServiceProxy = Objects.requireNonNull(backendServiceProxy, "backendServiceProxy can not be null");
        this.timeoutDeadline = coordinatorContext.timeoutDeadline.get();

        // flatten to fragment tasks to quickly index by BackendFragmentId, when receive the report message
        ImmutableMap.Builder<BackendFragmentId, SingleFragmentPipelineTask> backendFragmentTasks
                = ImmutableMap.builder();
        for (Entry<Long, MultiFragmentsPipelineTask> backendTask : fragmentTasks.entrySet()) {
            Long backendId = backendTask.getKey();
            for (Entry<Integer, SingleFragmentPipelineTask> fragmentIdToTask : backendTask.getValue()
                    .getChildrenTasks().entrySet()) {
                Integer fragmentId = fragmentIdToTask.getKey();
                SingleFragmentPipelineTask fragmentTask = fragmentIdToTask.getValue();
                backendFragmentTasks.put(new BackendFragmentId(backendId, fragmentId), fragmentTask);
            }
        }
    }

    @Override
    public void execute() throws Exception {
        coordinatorContext.withLock(() -> {
            sendAndWaitPhaseOneRpc();
            if (coordinatorContext.twoPhaseExecution()) {
                sendAndWaitPhaseTwoRpc();
            }
            return null;
        });
    }

    @Override
    public String toString() {
        return "SqlPipelineTask(\n"
                + childrenTasks.allTasks()
                    .stream()
                    .map(multiFragmentsPipelineTask -> "  " + multiFragmentsPipelineTask)
                    .collect(Collectors.joining(",\n"))
                + "\n)";
    }

    private void sendAndWaitPhaseOneRpc() throws UserException, RpcException {
        List<RpcInfo> rpcs = Lists.newArrayList();
        for (MultiFragmentsPipelineTask fragmentsTask : childrenTasks.allTasks()) {
            rpcs.add(new RpcInfo(
                    fragmentsTask,
                    DateTime.now().getMillis(),
                    fragmentsTask.sendPhaseOneRpc(coordinatorContext.twoPhaseExecution()))
            );
        }
        Map<TNetworkAddress, List<Long>> rpcPhase1Latency = waitPipelineRpc(rpcs,
                timeoutDeadline - System.currentTimeMillis(), "send fragments");

        coordinatorContext.updateProfileIfPresent(profile -> profile.updateFragmentRpcCount(rpcs.size()));
        coordinatorContext.updateProfileIfPresent(SummaryProfile::setFragmentSendPhase1Time);
        coordinatorContext.updateProfileIfPresent(profile -> profile.setRpcPhase1Latency(rpcPhase1Latency));
    }

    private void sendAndWaitPhaseTwoRpc() throws RpcException, UserException {
        List<RpcInfo> rpcs = Lists.newArrayList();
        for (MultiFragmentsPipelineTask fragmentTask : childrenTasks.allTasks()) {
            rpcs.add(new RpcInfo(
                    fragmentTask,
                    DateTime.now().getMillis(),
                    fragmentTask.sendPhaseTwoRpc())
            );
        }

        Map<TNetworkAddress, List<Long>> rpcPhase2Latency = waitPipelineRpc(rpcs,
                timeoutDeadline - System.currentTimeMillis(), "send execution start");
        coordinatorContext.updateProfileIfPresent(profile -> profile.updateFragmentRpcCount(rpcs.size()));
        coordinatorContext.updateProfileIfPresent(SummaryProfile::setFragmentSendPhase2Time);
        coordinatorContext.updateProfileIfPresent(profile -> profile.setRpcPhase2Latency(rpcPhase2Latency));
    }

    private Map<TNetworkAddress, List<Long>> waitPipelineRpc(
            List<RpcInfo> rpcs,
            long leftTimeMs, String operation) throws RpcException, UserException {
        TQueryOptions queryOptions = coordinatorContext.queryOptions;
        TUniqueId queryId = coordinatorContext.queryId;

        if (leftTimeMs <= 0) {
            long currentTimeMillis = System.currentTimeMillis();
            long elapsed = (currentTimeMillis - timeoutDeadline) / 1000 + queryOptions.getExecutionTimeout();
            String msg = String.format(
                    "timeout before waiting %s rpc, query timeout:%d, already elapsed:%d, left for this:%d",
                    operation, queryOptions.getExecutionTimeout(), elapsed, leftTimeMs);
            LOG.warn("Query {} {}", DebugUtil.printId(queryId), msg);
            if (!queryOptions.isSetExecutionTimeout() || !queryOptions.isSetQueryTimeout()) {
                LOG.warn("Query {} does not set timeout info, execution timeout: is_set:{}, value:{}"
                                + ", query timeout: is_set:{}, value: {}, "
                                + "coordinator timeout deadline {}, cur time millis: {}",
                        DebugUtil.printId(queryId),
                        queryOptions.isSetExecutionTimeout(), queryOptions.getExecutionTimeout(),
                        queryOptions.isSetQueryTimeout(), queryOptions.getQueryTimeout(),
                        timeoutDeadline, currentTimeMillis);
            }
            throw new UserException(msg);
        }

        // BE -> (RPC latency from FE to BE, Execution latency on bthread, Duration of doing work, RPC latency from BE
        // to FE)
        Map<TNetworkAddress, List<Long>> beToPrepareLatency = new HashMap<>();
        long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        for (RpcInfo rpc : rpcs) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;

            Backend backend = rpc.task.getBackend();
            long beId = backend.getId();
            TNetworkAddress brpcAddress = backend.getBrpcAddress();

            try {
                PExecPlanFragmentResult result = rpc.future.get(timeoutMs, TimeUnit.MILLISECONDS);
                long rpcDone = DateTime.now().getMillis();
                beToPrepareLatency.put(brpcAddress,
                        Lists.newArrayList(result.getReceivedTime() - rpc.startTime,
                                result.getExecutionTime() - result.getReceivedTime(),
                                result.getExecutionDoneTime() - result.getExecutionTime(),
                                rpcDone - result.getExecutionDoneTime()));
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code == null) {
                    code = TStatusCode.INTERNAL_ERROR;
                }

                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = operation + " failed. backend id: " + beId;
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
                backendServiceProxy.removeProxy(brpcAddress);
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
                backendServiceProxy.removeProxy(brpcAddress);
            } catch (TimeoutException e) {
                exception = e;
                errMsg = String.format(
                        "timeout when waiting for %s rpc, query timeout:%d, left timeout for this operation:%d",
                        operation, queryOptions.getExecutionTimeout(), timeoutMs / 1000);
                LOG.warn("Query {} {}", DebugUtil.printId(queryId), errMsg);
                code = TStatusCode.TIMEOUT;
                backendServiceProxy.removeProxy(brpcAddress);
            }

            if (code != TStatusCode.OK) {
                if (exception != null && errMsg == null) {
                    errMsg = operation + " failed. " + exception.getMessage();
                }
                Status cancelStatus = new Status(TStatusCode.INTERNAL_ERROR, errMsg);
                coordinatorContext.updateStatusIfOk(cancelStatus);
                coordinatorContext.cancelSchedule(cancelStatus);
                switch (code) {
                    case TIMEOUT:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(brpcAddress.hostname)
                                .increase(1L);
                        throw new RpcException(brpcAddress.hostname, errMsg, exception);
                    case THRIFT_RPC_ERROR:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(brpcAddress.hostname)
                                .increase(1L);
                        SimpleScheduler.addToBlacklist(beId, errMsg);
                        throw new RpcException(brpcAddress.hostname, errMsg, exception);
                    default:
                        throw new UserException(errMsg, exception);
                }
            }
        }
        return beToPrepareLatency;
    }

    private static class RpcInfo {
        public final MultiFragmentsPipelineTask task;
        public final long startTime;
        public final Future<PExecPlanFragmentResult> future;

        public RpcInfo(MultiFragmentsPipelineTask task, long startTime, Future<PExecPlanFragmentResult> future) {
            this.task = task;
            this.startTime = startTime;
            this.future = future;
        }
    }
}

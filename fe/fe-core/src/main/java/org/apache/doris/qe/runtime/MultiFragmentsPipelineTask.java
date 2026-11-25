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

import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentStartRequest;
import org.apache.doris.proto.Types;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.SimpleScheduler;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.BackendServiceProxy.BackendRpcTarget;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MultiFragmentsPipelineTask extends AbstractRuntimeTask<Integer, SingleFragmentPipelineTask> {
    private static final Logger LOG = LogManager.getLogger(PipelineExecutionTask.class);

    // immutable parameters
    private final CoordinatorContext coordinatorContext;
    private final Backend backend;
    private final BackendRpcTarget target;
    private final boolean needsFileEncryption;

    // mutable states

    // we will set fragmentsParams and serializeFragments to null after send rpc, to save memory
    private ByteString serializeFragments;
    private final AtomicBoolean hasCancelled;
    private final AtomicBoolean cancelInProcess;

    public MultiFragmentsPipelineTask(
            CoordinatorContext coordinatorContext, Backend backend, BackendServiceProxy backendClientProxy,
            ByteString serializeFragments,
            Map<Integer, SingleFragmentPipelineTask> fragmentTasks) {
        this(coordinatorContext, backend, backendClientProxy, serializeFragments, fragmentTasks, false);
    }

    public MultiFragmentsPipelineTask(
            CoordinatorContext coordinatorContext, Backend backend, BackendServiceProxy backendClientProxy,
            ByteString serializeFragments, Map<Integer, SingleFragmentPipelineTask> fragmentTasks,
            boolean needsFileEncryption) {
        this(coordinatorContext, backend, new BackendRpcTarget(backend.getBrpcAddress(), backendClientProxy),
                serializeFragments, fragmentTasks, needsFileEncryption);
    }

    public MultiFragmentsPipelineTask(
            CoordinatorContext coordinatorContext, Backend backend, BackendRpcTarget target,
            ByteString serializeFragments, Map<Integer, SingleFragmentPipelineTask> fragmentTasks) {
        this(coordinatorContext, backend, target, serializeFragments, fragmentTasks, false);
    }

    public MultiFragmentsPipelineTask(
            CoordinatorContext coordinatorContext, Backend backend, BackendRpcTarget target,
            ByteString serializeFragments, Map<Integer, SingleFragmentPipelineTask> fragmentTasks,
            boolean needsFileEncryption) {
        super(new ChildrenRuntimeTasks<>(fragmentTasks));
        this.needsFileEncryption = needsFileEncryption;
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
        this.backend = Objects.requireNonNull(backend, "backend can not be null");
        this.target = Objects.requireNonNull(target, "target can not be null");
        this.serializeFragments = Objects.requireNonNull(
                serializeFragments, "serializeFragments can not be null"
        );
        this.hasCancelled = new AtomicBoolean();
        this.cancelInProcess = new AtomicBoolean();
    }

    void removeProxy() {
        // Failed remote calls must evict the same endpoint policy used for prepare/start/cancel.
        target.remove();
    }

    public Future<PExecPlanFragmentResult> sendPhaseOneRpc(boolean twoPhaseExecution) {
        return execRemoteFragmentsAsync(
                serializeFragments, twoPhaseExecution
        );
    }

    public Future<PExecPlanFragmentResult> sendPhaseTwoRpc() {
        return execPlanFragmentStartAsync();
    }

    @Override
    public String toString() {
        TNetworkAddress brpcAddress = backend.getBrpcAddress();
        return "MultiFragmentsPipelineTask(Backend " + backend.getId()
                + "(" + brpcAddress.getHostname() + ":" + brpcAddress.getPort() + "): ["
                + childrenTasks.allTasks()
                    .stream()
                    .map(singleFragment -> "F" + singleFragment.getFragmentId())
                    .collect(Collectors.joining(", ")) + "])";
    }

    public synchronized void cancelExecute(Status cancelReason) {
        TUniqueId queryId = coordinatorContext.queryId;
        if (LOG.isDebugEnabled()) {
            LOG.debug("cancelRemoteFragments backend: {}, query={}, reason: {}",
                    backend, DebugUtil.printId(queryId), cancelReason.toString());
        }

        if (this.hasCancelled.get() || this.cancelInProcess.get()) {
            LOG.info("Fragment has already been cancelled. Query {} backend: {}",
                    DebugUtil.printId(queryId), backend);
            return;
        }
        try {
            TNetworkAddress brpcAddress = backend.getBrpcAddress();
            try {
                // Cancellation must use the same endpoint policy as fragment prepare/start.
                ListenableFuture<PCancelPlanFragmentResult> cancelResult =
                        target.cancel(queryId, cancelReason);
                Futures.addCallback(cancelResult, new FutureCallback<PCancelPlanFragmentResult>() {
                    public void onSuccess(InternalService.PCancelPlanFragmentResult result) {
                        try {
                            if (result.hasStatus()) {
                                Status status = new Status(result.getStatus());
                                if (status.getErrorCode() == TStatusCode.OK) {
                                    hasCancelled.set(true);
                                } else {
                                    LOG.warn("Failed to cancel query {} backend: {}, reason: {}",
                                            DebugUtil.printId(queryId), backend, status.toString());
                                }
                            } else {
                                LOG.warn("Failed to cancel query {} backend: {} reason: {}",
                                        DebugUtil.printId(queryId), backend, "without status");
                            }
                        } finally {
                            cancelInProcess.set(false);
                        }
                    }

                    public void onFailure(Throwable t) {
                        cancelInProcess.set(false);
                        LOG.warn("Failed to cancel query {} backend: {}, reason: {}",
                                DebugUtil.printId(queryId), backend,  cancelReason.toString(), t);
                    }
                }, Coordinator.backendRpcCallbackExecutor);
                cancelInProcess.set(true);
            } catch (RpcException e) {
                LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                        brpcAddress.getPort());
                SimpleScheduler.addToBlacklist(backend.getId(), e.getMessage());
            }
        } catch (Exception e) {
            LOG.warn("catch a exception", e);
        }
    }

    public Backend getBackend() {
        return backend;
    }

    private Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync(
            ByteString serializedFragments,
            boolean twoPhaseExecution) {
        Preconditions.checkNotNull(serializedFragments);
        try {
            // Serialization can precede a failed heartbeat or backend replacement; recheck at the send boundary.
            if (needsFileEncryption) {
                target.requireFileEncryption(backend.getId());
            }
            return target.prepare(serializedFragments, twoPhaseExecution);
        } catch (RpcException | IllegalStateException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return futureWithException(e);
        } finally {
            // save memory
            this.serializeFragments = null;
        }
    }

    public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync() {
        TUniqueId queryId = coordinatorContext.queryId;
        try {
            PExecPlanFragmentStartRequest.Builder builder = PExecPlanFragmentStartRequest.newBuilder();
            PUniqueId qid = PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
            builder.setQueryId(qid);
            return target.start(builder.build());
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return futureWithException(e);
        }
    }

    private Future<PExecPlanFragmentResult> futureWithException(Exception e) {
        // A lost reader/transport capability is a planning refusal, not a failed RPC. Misclassifying
        // it would blacklist an otherwise healthy backend and disrupt unrelated plaintext queries.
        TStatusCode code = e instanceof RpcException ? TStatusCode.THRIFT_RPC_ERROR : TStatusCode.NOT_IMPLEMENTED_ERROR;
        return new Future<PExecPlanFragmentResult>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public PExecPlanFragmentResult get() {
                PExecPlanFragmentResult result = PExecPlanFragmentResult.newBuilder().setStatus(
                        Types.PStatus.newBuilder().addErrorMsgs(e.getMessage())
                                .setStatusCode(code.getValue()).build()).build();
                return result;
            }

            @Override
            public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                return get();
            }
        };
    }
}

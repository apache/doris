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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;

import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Holds a waiting token between allocation and publication in Coordinator.enqueue(). */
public final class PausedAdmission implements AutoCloseable {
    private final CompletableFuture<QueueToken> waitingToken = new CompletableFuture<>();
    private final CountDownLatch publish = new CountDownLatch(1);
    private final CompletableFuture<Coordinator> firstCancel = new CompletableFuture<>();

    public PausedAdmission(QueryQueue queue) throws UserException {
        Mockito.doAnswer(call -> {
            QueueToken token = (QueueToken) call.callRealMethod();
            waitingToken.complete(token);
            if (!publish.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to publish the admission token");
            }
            return token;
        }).when(queue).getToken(1);
    }

    public static void useQueue(ConnectContext context, QueryQueue queue) throws UserException {
        WorkloadGroup group = Mockito.spy(context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context).get(0));
        Mockito.doReturn(queue).when(group).getQueryQueue();
        WorkloadGroupMgr groups = Mockito.mock(WorkloadGroupMgr.class);
        Mockito.when(groups.getWorkloadGroup(context)).thenReturn(List.of(group));
        Env env = Mockito.mock(Env.class, AdditionalAnswers.delegatesTo(context.getEnv()));
        Mockito.doReturn(groups).when(env).getWorkloadGroupMgr();
        context.setEnv(env);
    }

    // Install on the execution thread. Wait until the first cancel has checked the still-empty token.
    public MockedStatic<EnvFactory> observeCancellation(ConnectContext context) {
        EnvFactory factory = Mockito.spy(EnvFactory.getInstance());
        Mockito.doAnswer(call -> new Coordinator(context, call.getArgument(1), call.getArgument(2)) {
            @Override
            public void cancel(Status reason) {
                super.cancel(reason);
                firstCancel.complete(this);
            }
        }).when(factory).createCoordinator(Mockito.eq(context), Mockito.any(), Mockito.any());
        MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class);
        factories.when(EnvFactory::getInstance).thenReturn(factory);
        return factories;
    }

    public QueueToken awaitWaiter() throws Exception {
        return waitingToken.get(30, TimeUnit.SECONDS);
    }

    public Coordinator awaitFirstCancel() throws Exception {
        return firstCancel.get(10, TimeUnit.SECONDS);
    }

    public void publish() {
        publish.countDown();
    }

    @Override
    public void close() {
        // A failing assertion must also wake the old implementation, which can miss this token.
        waitingToken.thenAccept(QueueToken::cancel);
        publish();
    }
}

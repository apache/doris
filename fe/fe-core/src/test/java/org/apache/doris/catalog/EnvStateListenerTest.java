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

import org.apache.doris.common.util.Daemon;
import org.apache.doris.ha.FrontendNodeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class EnvStateListenerTest {
    @Test
    public void testInterruptedNonMasterTransitionDoesNotCommitFeType() throws Exception {
        Env env = Mockito.spy(new Env(false));
        setField(env, "replayer", Mockito.mock(Daemon.class));

        CountDownLatch firstTransitionInterrupted = new CountDownLatch(1);
        CountDownLatch repeatedTransitionAttempted = new CountDownLatch(1);
        AtomicInteger transitionAttempts = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (transitionAttempts.incrementAndGet() == 1) {
                firstTransitionInterrupted.countDown();
            } else {
                // Let runOneCycle return after the repeated FOLLOWER transition is interrupted. Without this
                // event, the state listener would correctly keep waiting for another state after the assertion.
                env.notifyNewFETypeTransfer(FrontendNodeType.INIT);
                repeatedTransitionAttempted.countDown();
            }
            return false;
        }).when(env).postProcessAfterMetadataReplayed(true);

        env.startStateListener();
        Daemon stateListener = (Daemon) getField(env, "listener");
        try {
            env.notifyNewFETypeTransfer(FrontendNodeType.FOLLOWER);
            Assertions.assertTrue(firstTransitionInterrupted.await(5, TimeUnit.SECONDS));

            // The first transition was interrupted before non-master initialization completed. A repeated
            // FOLLOWER event must retry the transition instead of being discarded as an already completed state.
            env.notifyNewFETypeTransfer(FrontendNodeType.FOLLOWER);
            Assertions.assertTrue(repeatedTransitionAttempted.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(FrontendNodeType.INIT, env.getFeType());
        } finally {
            stateListener.exit();
        }
    }

    private static Object getField(Env env, String fieldName) throws ReflectiveOperationException {
        Field field = Env.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(env);
    }

    private static void setField(Env env, String fieldName, Object value) throws ReflectiveOperationException {
        Field field = Env.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(env, value);
    }
}

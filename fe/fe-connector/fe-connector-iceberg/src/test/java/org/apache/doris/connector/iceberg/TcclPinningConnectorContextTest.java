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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Verifies the split-brain guard {@link TcclPinningConnectorContext} adds to the iceberg write/DDL/procedure
 * paths: WHY it exists is that iceberg-aws resolves {@code ApacheHttpClientConfigurations} via {@code DynMethods}
 * off the thread-context classloader while building the S3 client during a commit, so the commit MUST run with
 * the TCCL pinned to the plugin loader or it ClassCasts the parent (fe-core) copy against the child-loaded one.
 * These tests pin that contract: the task runs under the plugin loader, the caller's TCCL is always restored,
 * and the wrap stays transparent (delegates to the engine context, runs the task INSIDE its auth scope).
 */
public class TcclPinningConnectorContextTest {

    private static ClassLoader isolatedLoader() {
        return new URLClassLoader(new URL[0], TcclPinningConnectorContextTest.class.getClassLoader());
    }

    @Test
    public void pinsPluginLoaderForTheTaskThenRestoresCallerTccl() throws Exception {
        ClassLoader pluginLoader = isolatedLoader();
        ClassLoader callerLoader = isolatedLoader();
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, pluginLoader);

        Thread thread = Thread.currentThread();
        ClassLoader saved = thread.getContextClassLoader();
        thread.setContextClassLoader(callerLoader);
        try {
            ClassLoader[] seenDuringTask = new ClassLoader[1];
            String result = ctx.executeAuthenticated(() -> {
                seenDuringTask[0] = Thread.currentThread().getContextClassLoader();
                return "ok";
            });

            Assertions.assertEquals("ok", result);
            Assertions.assertSame(pluginLoader, seenDuringTask[0],
                    "the commit body must run with the TCCL pinned to the plugin loader");
            Assertions.assertSame(callerLoader, thread.getContextClassLoader(),
                    "the caller's TCCL must be restored after the call");
            Assertions.assertEquals(1, delegate.authCount,
                    "must delegate to the wrapped engine context's executeAuthenticated (1 wrap, not bypassed)");
        } finally {
            thread.setContextClassLoader(saved);
        }
    }

    @Test
    public void restoresCallerTcclWhenTheTaskThrows() {
        ClassLoader pluginLoader = isolatedLoader();
        ClassLoader callerLoader = isolatedLoader();
        TcclPinningConnectorContext ctx =
                new TcclPinningConnectorContext(new RecordingConnectorContext(), pluginLoader);

        Thread thread = Thread.currentThread();
        ClassLoader saved = thread.getContextClassLoader();
        thread.setContextClassLoader(callerLoader);
        try {
            Assertions.assertThrows(IllegalStateException.class, () ->
                    ctx.executeAuthenticated(() -> {
                        throw new IllegalStateException("boom");
                    }));
            Assertions.assertSame(callerLoader, thread.getContextClassLoader(),
                    "the caller's TCCL must be restored even when the commit body throws");
        } finally {
            thread.setContextClassLoader(saved);
        }
    }

    @Test
    public void runsTheTaskInsideTheDelegatesAuthScope() {
        // failAuth makes the delegate throw WITHOUT invoking the task; if the task still ran, the wrap would be
        // executing it OUTSIDE the auth scope. It must not.
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        delegate.failAuth = true;
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, isolatedLoader());

        boolean[] taskRan = {false};
        Assertions.assertThrows(RuntimeException.class, () ->
                ctx.executeAuthenticated(() -> {
                    taskRan[0] = true;
                    return null;
                }));
        Assertions.assertFalse(taskRan[0], "task must run inside the delegate's auth scope, not around it");
    }

    @Test
    public void delegatesNonAuthMethods() {
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, isolatedLoader());

        Assertions.assertEquals("test", ctx.getCatalogName());
        ctx.loadHiveConfResources("a,b");
        Assertions.assertTrue(delegate.hiveConfResourcesCalled, "loadHiveConfResources must reach the delegate");
        Assertions.assertEquals("a,b", delegate.lastHiveConfResourcesArg);
    }
}

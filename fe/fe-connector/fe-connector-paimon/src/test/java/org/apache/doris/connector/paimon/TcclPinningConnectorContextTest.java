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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;

/**
 * Verifies the {@link TcclPinningConnectorContext} decorator the paimon connector wraps its context in: the op
 * runs under the plugin loader, the caller's TCCL is always restored, and (single-owner auth) a Kerberos
 * catalog runs the op under the plugin authenticator's {@code doAs} WITHOUT also invoking the FE-injected
 * app-side authenticator. Mirrors the iceberg connector's {@code TcclPinningConnectorContextTest}; paimon has
 * no live Kerberos regression suite, so this wiring test is the offline gate.
 */
public class TcclPinningConnectorContextTest {

    private static ClassLoader isolatedLoader() {
        return new URLClassLoader(new URL[0], TcclPinningConnectorContextTest.class.getClassLoader());
    }

    @Test
    public void nonKerberosPinsPluginLoaderThenRestoresAndDelegatesAuth() throws Exception {
        ClassLoader pluginLoader = isolatedLoader();
        ClassLoader callerLoader = isolatedLoader();
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, pluginLoader, () -> null);

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
                    "the op must run with the TCCL pinned to the plugin loader");
            Assertions.assertSame(callerLoader, thread.getContextClassLoader(),
                    "the caller's TCCL must be restored after the call");
            Assertions.assertEquals(1, delegate.authCount,
                    "non-Kerberos (null authenticator) must delegate to the FE-injected executeAuthenticated");
        } finally {
            thread.setContextClassLoader(saved);
        }
    }

    @Test
    public void restoresCallerTcclWhenTheTaskThrows() {
        ClassLoader pluginLoader = isolatedLoader();
        ClassLoader callerLoader = isolatedLoader();
        TcclPinningConnectorContext ctx =
                new TcclPinningConnectorContext(new RecordingConnectorContext(), pluginLoader, () -> null);

        Thread thread = Thread.currentThread();
        ClassLoader saved = thread.getContextClassLoader();
        thread.setContextClassLoader(callerLoader);
        try {
            Assertions.assertThrows(IllegalStateException.class, () ->
                    ctx.executeAuthenticated(() -> {
                        throw new IllegalStateException("boom");
                    }));
            Assertions.assertSame(callerLoader, thread.getContextClassLoader(),
                    "the caller's TCCL must be restored even when the task throws");
        } finally {
            thread.setContextClassLoader(saved);
        }
    }

    @Test
    public void kerberosRunsTaskInPluginDoAsAndBypassesDelegateAuth() throws Exception {
        // Single-owner auth (Option A): a Kerberos catalog runs the op under the PLUGIN authenticator's doAs and
        // must NOT ALSO invoke the FE-injected app-side authenticator (delegate.executeAuthenticated), which only
        // authenticates the unused app-loader UGI copy. WHY it matters: the plugin's FileSystem reads the plugin
        // UGI, so the plugin doAs is the only auth that reaches secured HDFS. MUTATION: nesting inside the
        // delegate (authCount == 1) or skipping the plugin doAs (doAsCount == 0) -> red.
        ClassLoader pluginLoader = isolatedLoader();
        ClassLoader callerLoader = isolatedLoader();
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        RecordingAuthenticator auth = new RecordingAuthenticator();
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, pluginLoader, () -> auth);

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
            Assertions.assertEquals(1, auth.doAsCount, "the op must run inside the plugin authenticator's doAs");
            Assertions.assertEquals(0, delegate.authCount,
                    "single-owner: the FE-injected app-side authenticator must NOT be invoked on the Kerberos path");
            Assertions.assertSame(pluginLoader, seenDuringTask[0],
                    "the op must still run with the TCCL pinned to the plugin loader");
            Assertions.assertSame(callerLoader, thread.getContextClassLoader(),
                    "the caller's TCCL must be restored");
        } finally {
            thread.setContextClassLoader(saved);
        }
    }

    @Test
    public void delegatesSiblingConnectorToTheRawContext() {
        // createSiblingConnector is a non-auth engine-service method: the decorator must forward it to the raw
        // delegate (else a wrapped gateway context would return the SPI default null, masking a real sibling as
        // "provider missing"). Assert the type + props reach the delegate unchanged.
        RecordingConnectorContext delegate = new RecordingConnectorContext();
        TcclPinningConnectorContext ctx = new TcclPinningConnectorContext(delegate, isolatedLoader(), () -> null);

        Map<String, String> siblingProps = Collections.singletonMap("iceberg.catalog.type", "hms");
        ctx.createSiblingConnector("iceberg", siblingProps);

        Assertions.assertEquals("iceberg", delegate.lastSiblingType,
                "createSiblingConnector type must reach the delegate (decorator is an exhaustive pass-through)");
        Assertions.assertSame(siblingProps, delegate.lastSiblingProps,
                "createSiblingConnector properties must reach the delegate unchanged");
    }

    /** Wiring-only {@link HadoopAuthenticator} double: records doAs calls and runs the action WITHOUT a UGI. */
    private static final class RecordingAuthenticator implements HadoopAuthenticator {
        int doAsCount;

        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("wiring double: getUGI is unused (doAs is overridden)");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            doAsCount++;
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}

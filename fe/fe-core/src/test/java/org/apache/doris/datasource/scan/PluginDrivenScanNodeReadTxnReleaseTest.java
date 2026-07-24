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

package org.apache.doris.datasource.scan;

import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards {@link PluginDrivenScanNode#buildReadTransactionReleaseCallback}, the query-finish callback that
 * releases a connector's per-query read transaction. Hive full-ACID / insert-only reads open a metastore read
 * transaction + shared read lock during {@code planScan}; the engine registers this callback in
 * {@code getSplits} to commit it (releasing the lock) when the query finishes. Without the release the shared
 * read lock leaks for the metastore's lifetime.
 *
 * <p><b>Why this matters:</b> the callback runs on the StmtExecutor thread at query finish, whose TCCL is the
 * fe-core app loader. {@code releaseReadTransaction -> txn.commit -> hmsClient.commitTxn} resolves
 * metastore/thrift classes by name via the TCCL, so the callback MUST pin the provider's plugin classloader for
 * the duration of the release, else it split-brains against the app loader's duplicate copies (ClassCast /
 * NoClassDef at commit). Each test kills a mutation: (1) dropping the release call / wrong queryId, (2) dropping
 * the TCCL pin, (3) leaking the pin.</p>
 */
public class PluginDrivenScanNodeReadTxnReleaseTest {

    @Test
    public void callbackReleasesReadTransactionForTheQuery() {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);

        Runnable callback = PluginDrivenScanNode.buildReadTransactionReleaseCallback(provider, "query-42");
        // The transaction is released only when the query finishes: merely building the callback must not
        // release it yet (a mutation that releases eagerly would leak nothing but breaks the deferred contract).
        Mockito.verifyNoInteractions(provider);

        callback.run();
        // MUTATION: dropping the releaseReadTransaction call, or passing a different queryId than the one the
        // txn was registered under (so deregister would miss it), is killed here.
        Mockito.verify(provider).releaseReadTransaction("query-42");
    }

    @Test
    public void callbackPinsProviderClassLoaderDuringReleaseAndRestoresAfter() throws Exception {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        ClassLoader providerLoader = provider.getClass().getClassLoader();

        AtomicReference<ClassLoader> tcclDuringRelease = new AtomicReference<>();
        Mockito.doAnswer(inv -> {
            tcclDuringRelease.set(Thread.currentThread().getContextClassLoader());
            return null;
        }).when(provider).releaseReadTransaction(Mockito.anyString());

        Thread current = Thread.currentThread();
        ClassLoader outer = current.getContextClassLoader();
        // A distinct sentinel TCCL (never equal to the provider's mock loader), so a "no pin" mutation — which
        // would leave the sentinel in place during the release — is caught by the assertSame below.
        try (URLClassLoader sentinel = new URLClassLoader(new URL[0], null)) {
            current.setContextClassLoader(sentinel);

            PluginDrivenScanNode.buildReadTransactionReleaseCallback(provider, "q").run();

            // MUTATION: dropping the onPluginClassLoader pin is killed — during the release the TCCL must be the
            // provider's (plugin) classloader, not the caller's app/sentinel loader.
            Assertions.assertSame(providerLoader, tcclDuringRelease.get(),
                    "the release must run with the TCCL pinned to the provider's plugin classloader");
            // MUTATION: leaking the pin (not restoring in a finally) is killed — the caller's TCCL must be back.
            Assertions.assertSame(sentinel, current.getContextClassLoader(),
                    "the TCCL must be restored to the caller's after the release");
        } finally {
            current.setContextClassLoader(outer);
        }
    }
}

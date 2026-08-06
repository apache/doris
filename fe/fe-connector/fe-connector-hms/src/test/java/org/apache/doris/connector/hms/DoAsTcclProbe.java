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

package org.apache.doris.connector.hms;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Test entrypoint, invoked reflectively THROUGH an isolated child-first classloader (see
 * {@link ThriftHmsClientDoAsClassLoaderTest}). Because the child-first loader defines this class — and
 * therefore {@link ThriftHmsClient} — itself, {@code ThriftHmsClient.class.getClassLoader()} inside here is
 * that isolated loader, distinct from the system classloader. That is what reproduces the production two-copy
 * topology (fe-core hadoop on the system loader, plugin hadoop child-first) so the difference between
 * {@code doAs} pinning {@code getClass().getClassLoader()} (correct) and {@code getSystemClassLoader()} (the
 * split-brain bug) becomes observable in a unit test.
 */
public final class DoAsTcclProbe {

    private DoAsTcclProbe() {
    }

    /**
     * Drives one metastore call and inspects the TCCL {@code doAs} pinned while creating the client. Returns
     * {@code "OK"} iff {@code doAs} pinned the connector's own (this isolated) loader AND restored the caller's
     * TCCL; otherwise a diagnostic string. Returning a plain String avoids any cross-loader type coupling with
     * the invoking test.
     */
    public static String check() throws Exception {
        // A fake IMetaStoreClient (no Mockito): List-returning calls yield an empty list, else null.
        InvocationHandler handler = (proxy, method, args) ->
                List.class.isAssignableFrom(method.getReturnType()) ? new ArrayList<>() : null;
        IMetaStoreClient fake = (IMetaStoreClient) Proxy.newProxyInstance(
                DoAsTcclProbe.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class}, handler);

        final ClassLoader[] observedDuringCreate = new ClassLoader[1];
        ThriftHmsClient.MetaStoreClientProvider provider = hiveConf -> {
            if (observedDuringCreate[0] == null) {
                observedDuringCreate[0] = Thread.currentThread().getContextClassLoader();
            }
            return fake;
        };

        // poolSize 0 -> no pool: borrowClient() -> createFreshClient() -> doAs() -> provider.create().
        HmsClientConfig config = new HmsClientConfig(new HashMap<>(), 0);
        ThriftHmsClient client = new ThriftHmsClient(config, null, provider, HmsTypeMapping.Options.DEFAULT);

        ClassLoader connectorLoader = ThriftHmsClient.class.getClassLoader();
        ClassLoader system = ClassLoader.getSystemClassLoader();
        // Drive under a DISTINCT caller TCCL so both "pinned to system" and "never pinned" are observable.
        ClassLoader caller = new URLClassLoader(new URL[0], connectorLoader);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(caller);
        try {
            client.listDatabases();
        } finally {
            ClassLoader afterCall = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(original);
            client.close();
            if (observedDuringCreate[0] == null) {
                return "NO_CLIENT_CREATED";
            }
            if (observedDuringCreate[0] == system && system != connectorLoader) {
                return "PIN_WRONG_SYSTEM_LOADER";
            }
            if (observedDuringCreate[0] == caller) {
                return "PIN_MISSING_LEFT_CALLER_LOADER";
            }
            if (observedDuringCreate[0] != connectorLoader) {
                return "PIN_WRONG_OTHER_LOADER";
            }
            if (afterCall != caller) {
                return "TCCL_NOT_RESTORED";
            }
        }
        return "OK";
    }
}

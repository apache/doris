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

import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ForwardingConnectorContext;
import org.apache.doris.kerberos.HadoopAuthenticator;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * A {@link ConnectorContext} decorator that wraps every {@link #executeAuthenticated} call, then delegates to
 * the wrapped engine context. Every other method is forwarded verbatim by
 * {@link ForwardingConnectorContext} — which is the point of extending it rather than implementing the
 * interface and copying each method by hand: a missed pass-through would not fail to compile, it would
 * quietly land on the interface default (a silent downgrade) instead of the engine. The paimon analogue of the
 * iceberg
 * connector's {@code TcclPinningConnectorContext}, wrapping the single FE-injected context once covers every
 * remote read/DDL/commit ({@code PaimonConnectorMetadata} routes them all through
 * {@link ConnectorContext#executeAuthenticated}).
 *
 * <p>TCCL: the pin keeps reflective loads on the plugin side for the duration of each op. The paimon plugin
 * bundles paimon-core + {@code hadoop-common}/{@code hadoop-hdfs-client} child-first, so any name-based
 * reflective load that defaults to the thread-context classloader would otherwise resolve the parent (fe-core)
 * copy and ClassCast against the child-loaded plugin copy — the same split-brain guard the iceberg connector
 * applies. The pin is harmless for pure reads (it just runs them under the plugin loader).
 *
 * <p>KERBEROS (single-owner auth): for a Kerberos catalog {@code pluginAuthenticator} supplies a plugin-side
 * {@link HadoopAuthenticator} and the op runs inside its {@code doAs}. This is REQUIRED because the plugin
 * bundles its own {@code hadoop-common} + {@code fe-kerberos} child-first, so the plugin's HDFS
 * {@code FileSystem} reads a DIFFERENT {@code UserGroupInformation} copy than the one the FE-injected
 * authenticator (built app-side, outside the plugin loader) logs in — the app-side
 * {@code doAs} therefore never reaches the plugin FileSystem, which falls back to SIMPLE auth. The connector
 * is the only party that knows which UGI copy its FileSystem uses, so it owns the auth: on the Kerberos path
 * we run the plugin {@code doAs} and DELIBERATELY do NOT also call {@code delegate.executeAuthenticated}
 * (which only authenticates the unused app-loader UGI — dead weight plus a redundant keytab login). The
 * plugin {@code doAs} mirrors {@code HadoopExecutionAuthenticator.execute}
 * ({@code hadoopAuthenticator.doAs(task::call)}), so exception semantics are unchanged. When the supplier
 * returns {@code null} (non-Kerberos) the FE-injected path is preserved byte-for-byte.
 *
 * <p>Note: paimon has no live Kerberos regression suite, so this is verified by wiring/static reasoning; the
 * end-to-end gate is the iceberg Kerberos suite, which exercises the identical mechanism.
 */
final class TcclPinningConnectorContext extends ForwardingConnectorContext {

    private final ClassLoader pluginClassLoader;
    private final Supplier<HadoopAuthenticator> pluginAuthenticator;

    TcclPinningConnectorContext(ConnectorContext delegate, ClassLoader pluginClassLoader,
            Supplier<HadoopAuthenticator> pluginAuthenticator) {
        super(delegate);
        this.pluginClassLoader = Objects.requireNonNull(pluginClassLoader, "pluginClassLoader");
        this.pluginAuthenticator = Objects.requireNonNull(pluginAuthenticator, "pluginAuthenticator");
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader);
            HadoopAuthenticator auth = pluginAuthenticator.get();
            if (auth == null) {
                // Non-Kerberos: keep the FE-injected auth path exactly as-is.
                return delegate().executeAuthenticated(task);
            }
            // Kerberos: the connector is the sole authenticator. Run the op under the PLUGIN's UGI copy (the
            // one the plugin's FileSystem reads); do NOT also invoke the FE-injected app-side authenticator.
            return auth.doAs(task::call);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    // Every other method is forwarded by ForwardingConnectorContext. Only methods that must run
    // under the plugin loader (or under the plugin's own authenticator) belong here.
    //
    // createSiblingConnector deliberately reaches the RAW engine context rather than this wrapper:
    // the sibling applies its own TCCL/auth pinning to whatever context it is handed, so handing it a
    // context already pinned to THIS plugin's loader would pin it to the wrong plugin. The base class
    // forwards to the wrapped context, which is exactly that.
}

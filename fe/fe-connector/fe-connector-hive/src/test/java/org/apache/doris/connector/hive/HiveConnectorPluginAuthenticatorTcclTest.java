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

package org.apache.doris.connector.hive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

/**
 * Tests that {@link HiveConnector#buildPluginAuthenticator(java.util.Map)} runs under the plugin (child-first)
 * classloader (TCCL pinned), then restores the caller's TCCL.
 *
 * <p>WHY: a catalog that declares {@code hadoop.security.authentication=kerberos} WITHOUT a principal/keytab
 * falls back to a {@code HadoopSimpleAuthenticator}, whose constructor EAGERLY calls
 * {@code UserGroupInformation.createRemoteUser} → {@code SecurityUtil.<clinit>} — whose internal
 * {@code new Configuration()} captures the current TCCL. This resolution runs on the (unpinned) createClient
 * thread, so an unpinned TCCL would load hadoop's {@code DNSDomainNameResolver} from fe-core's system-loader
 * copy and split-brain-poison {@code SecurityUtil} against the plugin's copy (the same failure
 * {@code ThriftHmsClient.doAs} guards). This is the latent-edge companion to that fix.
 *
 * <p>The map records the TCCL the first time {@code buildPluginAuthenticator} reads a key (its very first
 * statement), which is inside the pinned region. Simple-auth properties are used so the method returns quickly
 * without an eager UGI side effect, yet still exercises the pin. Without the pin the observed loader would be
 * the caller's marker — so the assertion is RED on a missing pin, and on a dropped restore.
 */
public class HiveConnectorPluginAuthenticatorTcclTest {

    /** A properties map that records the TCCL the first time a key is read. */
    private static final class TcclRecordingMap extends HashMap<String, String> {
        private static final long serialVersionUID = 1L;
        private transient ClassLoader observed;

        @Override
        public String get(Object key) {
            if (observed == null) {
                observed = Thread.currentThread().getContextClassLoader();
            }
            return super.get(key);
        }
    }

    @Test
    public void buildPluginAuthenticatorRunsUnderPluginClassLoaderAndRestores() {
        TcclRecordingMap props = new TcclRecordingMap();
        props.put("hive.metastore.uris", "thrift://hms:9083");

        ClassLoader marker = new URLClassLoader(new URL[0], getClass().getClassLoader());
        ClassLoader pluginLoader = HiveConnector.class.getClassLoader();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(marker);
        try {
            HiveConnector.buildPluginAuthenticator(props);

            Assertions.assertNotNull(props.observed, "buildPluginAuthenticator must have read the properties");
            Assertions.assertSame(pluginLoader, props.observed,
                    "buildPluginAuthenticator must run under the plugin classloader, so an eager "
                            + "HadoopSimpleAuthenticator UGI init cannot split-brain SecurityUtil");
            Assertions.assertNotSame(marker, props.observed,
                    "buildPluginAuthenticator must re-pin the TCCL away from the caller's loader");
            Assertions.assertSame(marker, Thread.currentThread().getContextClassLoader(),
                    "buildPluginAuthenticator must restore the caller's TCCL (try/finally)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }
}

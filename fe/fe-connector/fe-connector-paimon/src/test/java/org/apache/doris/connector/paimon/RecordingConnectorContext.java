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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Hand-written {@link ConnectorContext} test double (no Mockito) used to assert that the
 * Paimon DDL path wraps every remote call in {@link #executeAuthenticated}.
 *
 * <p>Read-path tests just pass a fresh instance and ignore it. DDL tests assert on
 * {@link #authCount} (one wrap per DDL op) and use {@link #failAuth} to simulate an auth
 * failure: when set, {@link #executeAuthenticated} throws WITHOUT invoking the task, which
 * proves the seam call sits INSIDE the authenticator (if the production code called the seam
 * directly, the recording fake would log the call despite the auth failure).
 */
final class RecordingConnectorContext implements ConnectorContext {

    int authCount;
    boolean failAuth;

    // ---- FIX-HMS-CONFRES: loadHiveConfResources hook ----
    /** Map the fake returns from {@link #loadHiveConfResources} (the "resolved" hive-site.xml keys). */
    Map<String, String> hiveConfResources = Collections.emptyMap();
    /** Whether the connector invoked {@link #loadHiveConfResources}. */
    boolean hiveConfResourcesCalled;
    /** The {@code resources} string the connector passed to {@link #loadHiveConfResources}. */
    String lastHiveConfResourcesArg;

    // ---- FIX-URI-NORMALIZE: normalizeStorageUri hook ----
    /** Number of times the connector invoked {@link #normalizeStorageUri}. */
    int normalizeCount;

    @Override
    public String getCatalogName() {
        return "test";
    }

    @Override
    public String normalizeStorageUri(String rawUri) {
        normalizeCount++;
        // Deterministic stand-in for the engine's oss://->s3:// scheme rewrite, so a connector wiring
        // test can prove BOTH the data-file and DV paths were routed through this hook (the real
        // normalization is covered by DefaultConnectorContextNormalizeUriTest in fe-core).
        if (rawUri != null && rawUri.startsWith("oss://")) {
            return "s3://" + rawUri.substring("oss://".length());
        }
        return rawUri;
    }

    @Override
    public Map<String, String> loadHiveConfResources(String resources) {
        hiveConfResourcesCalled = true;
        lastHiveConfResourcesArg = resources;
        return hiveConfResources;
    }

    @Override
    public long getCatalogId() {
        return 0;
    }

    @Override
    public <T> T executeAuthenticated(Callable<T> task) throws Exception {
        authCount++;
        if (failAuth) {
            // Deliberately do NOT call task -> the wrapped seam call must not run.
            throw new RuntimeException("auth failed");
        }
        return task.call();
    }
}

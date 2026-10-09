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

package org.apache.doris.connector.hudi;

import java.util.concurrent.Callable;

/**
 * Runs a Hudi {@code HoodieTableMetaClient}-touching action under the plugin's Kerberos UGI {@code doAs} - or,
 * for a non-Kerberos catalog, the connector's per-configuration filesystem scope - and a TCCL pin to the hudi
 * plugin classloader.
 *
 * <p>Built by {@link HudiConnector} and injected into {@link HudiConnectorMetadata} and
 * {@link HudiScanPlanProvider}. The metadata methods build a live metaClient off the query-planning /
 * MTMV-refresh thread, NOT the TCCL-pinned scan thread ({@code PluginDrivenScanNode.onPluginClassLoader}),
 * so they need the pin to resolve hudi-bundled reflection against the plugin's child-first copies and to
 * authenticate to a secured HMS/HDFS (post-flip the FE-injected {@code context.executeAuthenticated} is NOOP
 * for a sibling). The scan provider is already pinned by the engine and needs the OTHER half: the engine runs
 * it under no {@code doAs} at all, and the UGI current while a metaClient opens its filesystems decides where
 * Hadoop caches them - and therefore whether {@code HudiConnector.close()} can ever close them. See
 * {@code HudiConnector.metaClientExecutor()} and memory {@code catalog-spi-plugin-tccl-classloader-gotcha}.</p>
 *
 * <p>A generic method (not a lambda target): the implementation is an anonymous class in {@link HudiConnector}.
 * Checked exceptions from {@code action} are wrapped by the implementation.</p>
 */
interface HudiMetaClientExecutor {
    <T> T execute(Callable<T> action);

    /**
     * Runs the action on the calling thread as it is - no pin, no {@code doAs}. For the pure-helper tests of
     * {@link HudiScanPlanProvider}; a checked exception surfaces as an unchecked wrapper so a mis-set-up
     * fixture fails loud.
     */
    static HudiMetaClientExecutor inline() {
        return new HudiMetaClientExecutor() {
            @Override
            public <T> T execute(Callable<T> action) {
                try {
                    return action.call();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }
}

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
 * Runs a Hudi {@code HoodieTableMetaClient}-touching action under the plugin's Kerberos UGI {@code doAs} and a
 * TCCL pin to the hudi plugin classloader.
 *
 * <p>Built by {@link HudiConnector} and injected into {@link HudiConnectorMetadata} so the partition-listing /
 * MVCC-snapshot metadata methods — which build a live metaClient off the query-planning / MTMV-refresh thread,
 * NOT the TCCL-pinned scan thread ({@code PluginDrivenScanNode.onPluginClassLoader}) — resolve hudi-bundled
 * reflection against the plugin's child-first copies and authenticate to a secured HMS/HDFS (post-flip the
 * FE-injected {@code context.executeAuthenticated} is NOOP for a sibling). See
 * {@code HudiConnector.metaClientExecutor()} and memory {@code catalog-spi-plugin-tccl-classloader-gotcha}.</p>
 *
 * <p>A generic method (not a lambda target): the implementation is an anonymous class in {@link HudiConnector}.
 * Checked exceptions from {@code action} are wrapped by the implementation.</p>
 */
interface HudiMetaClientExecutor {
    <T> T execute(Callable<T> action);
}

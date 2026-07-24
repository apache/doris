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

package org.apache.doris.connector.api.scan;

import java.io.Closeable;

/**
 * A lazy, closeable source of {@link ConnectorScanRange}s for streaming (batched) split generation,
 * echoing Trino's {@code ConnectorSplitSource}. Returned by
 * {@link ConnectorScanPlanProvider#streamSplits} when a connector opts into streaming split
 * generation (see {@link ConnectorScanPlanProvider#streamingSplitEstimate}).
 *
 * <p>The engine (e.g. {@code PluginDrivenScanNode}) pulls ranges incrementally with backpressure
 * and pumps them into the split queue, instead of materializing all ranges up front via
 * {@link ConnectorScanPlanProvider#planScan}. This bounds FE heap usage for very large scans
 * (mirrors legacy {@code IcebergScanNode.doStartSplit}).</p>
 *
 * <p>Implementations MUST defer the heavy planning (e.g. iceberg {@code TableScan.planFiles()})
 * until ranges are actually consumed, and MUST release the underlying resources in {@link #close()}.
 * Instances are single-pass and not thread-safe; the engine drives one source from a single
 * background task.</p>
 */
public interface ConnectorSplitSource extends Closeable {

    /**
     * Returns whether more ranges remain. May advance over internally-skipped tasks (e.g. files
     * filtered out by a rewrite scope), so it is the only safe way to test for completion.
     */
    boolean hasNext();

    /**
     * Returns the next range. Must only be called when {@link #hasNext()} is {@code true}.
     */
    ConnectorScanRange next();
}

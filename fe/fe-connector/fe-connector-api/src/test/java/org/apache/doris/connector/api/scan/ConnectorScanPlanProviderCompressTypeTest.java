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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.thrift.TFileCompressType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Guards the additive {@code adjustFileCompressType} SPI default on {@link ConnectorScanPlanProvider}.
 *
 * <p>WHY: the default MUST be identity so no connector's inferred file compression type is silently altered
 * — only a connector that explicitly opts in (hive/hudi remap {@code LZ4FRAME -> LZ4BLOCK}) changes it. If the
 * default ever stopped being identity, every non-opting connector's reads would ship a different compress type
 * to BE. This is the zero-break guard for es/jdbc/paimon/iceberg/trino/maxcompute.</p>
 */
public class ConnectorScanPlanProviderCompressTypeTest {

    /** Bare provider: only the abstract 4-arg planScan implemented; everything else inherits SPI defaults. */
    private static final class BareProvider implements ConnectorScanPlanProvider {
        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
            return Collections.emptyList();
        }
    }

    @Test
    public void testAdjustFileCompressTypeDefaultsToIdentity() {
        ConnectorScanPlanProvider provider = new BareProvider();
        // The default must NOT touch any type — including LZ4FRAME, the one hive/hudi opt in to remap.
        for (TFileCompressType type : TFileCompressType.values()) {
            Assertions.assertEquals(type, provider.adjustFileCompressType(type),
                    "default adjustFileCompressType must be identity for " + type);
        }
    }
}

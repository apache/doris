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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@link ConnectorWritePlanProvider#getWriteSortColumns} default.
 *
 * <p><b>WHY this matters:</b> the generic translator asks every write-capable connector for its
 * write-sort columns. The default MUST be an empty list so connectors that declare no write sort
 * (jdbc / maxcompute) produce no {@code TSortInfo} and keep their byte-identical unsorted sink output —
 * only iceberg (with a {@code WRITE ORDERED BY}) overrides it.</p>
 */
public class ConnectorWritePlanProviderDefaultsTest {

    /** Minimal provider that overrides only the mandatory {@code planWrite}. */
    private static final class BareWritePlanProvider implements ConnectorWritePlanProvider {
        @Override
        public ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle) {
            return null;
        }
    }

    @Test
    public void writeSortColumnsDefaultsToNull() {
        ConnectorTableHandle anyTable = new ConnectorTableHandle() {
        };
        Assertions.assertNull(new BareWritePlanProvider().getWriteSortColumns(null, anyTable),
                "a connector that declares no write sort order must return null (not an empty list, which "
                        + "would signal a present-but-empty sort order) so its sink output stays "
                        + "byte-identical with no engine-built TSortInfo");
    }
}

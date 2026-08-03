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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@link ConnectorWriteSortColumn} carrier shape.
 *
 * <p><b>WHY this matters:</b> it is the engine-neutral vocabulary by which a connector declares its
 * write-side sort (T06, iceberg {@code WRITE ORDERED BY}). {@code columnIndex} is a position into the
 * sink's full-schema output (the same indexing {@code PhysicalConnectorTableSink} uses for write
 * distribution), so the generic translator can resolve it to a bound slot and build a {@code TSortInfo}
 * without knowing any connector's sort dialect. The three fields map 1:1 onto the legacy iceberg sink's
 * {@code orderingExprs}/{@code isAscOrder}/{@code isNullsFirst} triple.</p>
 */
public class ConnectorWriteSortColumnTest {

    @Test
    public void carriesColumnIndexAscAndNullsFirst() {
        ConnectorWriteSortColumn col = new ConnectorWriteSortColumn(3, true, false);
        Assertions.assertEquals(3, col.getColumnIndex());
        Assertions.assertTrue(col.isAsc());
        Assertions.assertFalse(col.isNullsFirst());
    }

    @Test
    public void carriesDescendingNullsFirst() {
        ConnectorWriteSortColumn col = new ConnectorWriteSortColumn(0, false, true);
        Assertions.assertEquals(0, col.getColumnIndex());
        Assertions.assertFalse(col.isAsc());
        Assertions.assertTrue(col.isNullsFirst());
    }
}

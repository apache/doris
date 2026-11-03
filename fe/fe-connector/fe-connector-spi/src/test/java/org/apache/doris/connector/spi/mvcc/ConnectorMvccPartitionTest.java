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

package org.apache.doris.connector.spi.mvcc;

import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class ConnectorMvccPartitionTest {
    @Test
    public void invalidNegativeRowsFail() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConnectorMvccPartition("p1",
                Collections.singletonList("2024-01-01"), Collections.singletonList("2024-01-02"), 1, -2));
    }

    @Test
    public void oldConstructorLeavesRowsUnknown() {
        ConnectorMvccPartition partition = new ConnectorMvccPartition("p1", Collections.singletonList("2024-01-01"),
                Collections.singletonList("2024-01-02"), 1);
        Assertions.assertEquals(ConnectorPartitionInfo.UNKNOWN, partition.getRowCount());
    }

    @Test
    public void knownZeroDiffersFromUnavailableRows() {
        ConnectorMvccPartition zero = new ConnectorMvccPartition("p1", Collections.singletonList("2024-01-01"),
                Collections.singletonList("2024-01-02"), 1, 0);
        ConnectorMvccPartition unavailable = new ConnectorMvccPartition("p1",
                zero.getLowerBound(), zero.getUpperBound(), 1);
        Assertions.assertEquals(0, zero.getRowCount());
        Assertions.assertNotEquals(zero, unavailable);
    }
}

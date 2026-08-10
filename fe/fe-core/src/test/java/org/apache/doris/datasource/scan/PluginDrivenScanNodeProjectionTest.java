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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class PluginDrivenScanNodeProjectionTest {

    @Test
    public void resolvedNestedAccessPathIsCarriedToConnectorHandle() {
        AtomicReference<Set<Integer>> observed = new AtomicReference<>();
        ConnectorColumnHandle handle = new ConnectorColumnHandle() {
            @Override
            public ConnectorColumnHandle withProjectedFieldIds(Set<Integer> projectedFieldIds) {
                observed.set(projectedFieldIds);
                return this;
            }

            @Override
            public int hashCode() {
                return 1;
            }

            @Override
            public boolean equals(Object other) {
                return this == other;
            }
        };
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), new TupleId(1));
        slot.setAllAccessPaths(ImmutableList.of(
                ColumnAccessPath.data(ImmutableList.of("10", "*", "11"))));

        Assertions.assertSame(handle, PluginDrivenScanNode.withProjectedFieldIds(handle, slot));
        Assertions.assertEquals(ImmutableSet.of(10, 11), observed.get());
    }
}

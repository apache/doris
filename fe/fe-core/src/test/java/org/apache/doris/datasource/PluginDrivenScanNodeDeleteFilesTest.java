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

package org.apache.doris.datasource;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

/**
 * FIX-E (explain gap) — guards {@link PluginDrivenScanNode#getDeleteFiles(TFileRangeDesc)}, the
 * override the SPI scan path was missing. The VERBOSE per-backend EXPLAIN block (inherited from
 * {@code FileScanNode}) calls {@code getDeleteFiles(rangeDesc)} to count deletion files; without this
 * override it returned empty, so {@code deleteFileNum} was always 0 and the {@code deleteFileNum}
 * substring never appeared ({@code test_paimon_deletion_vector_oss} asserts it is present).
 *
 * <p><b>Why this matters (Rule 9):</b> the override must DELEGATE to the connector's
 * {@link ConnectorScanPlanProvider#getDeleteFiles(TTableFormatFileDesc)} (paimon reads its deletion
 * vector off the per-range thrift), and must null-guard a range with no table-format params (legacy
 * {@code PaimonScanNode.getDeleteFiles} parity) so the VERBOSE loop never NPEs. Driven on a
 * {@code CALLS_REAL_METHODS} mock with the {@code connector} field injected (no full
 * {@code FileQueryScanNode} constructor needed; the method is package/protected exactly to enable
 * this, mirroring {@code PluginDrivenScanNodeSysHandleTest}'s Deencapsulation approach).</p>
 */
public class PluginDrivenScanNodeDeleteFilesTest {

    private static PluginDrivenScanNode nodeWithProvider(ConnectorScanPlanProvider provider) {
        PluginDrivenScanNode node =
                Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Connector connector = Mockito.mock(Connector.class);
        // The node resolves the provider PER TABLE via getScanPlanProvider(currentHandle); a real connector
        // delegates that overload to the no-arg getter, so the mock answers the arg form (currentHandle is
        // null in this partial node, hence the null-tolerant any() matcher).
        Mockito.when(connector.getScanPlanProvider(Mockito.any())).thenReturn(provider);
        Deencapsulation.setField(node, "connector", connector);
        return node;
    }

    @Test
    public void delegatesToProviderWithTableFormatParams() {
        // WHY: the node must hand the range's table-format params to the connector, which reads the
        // paimon deletion-file path back off them. MUTATION: an override that returns empty (no
        // delegation) makes deleteFileNum always 0 -> red. The distinct returned list proves the
        // connector's result flows through, and verify() proves the exact params were passed.
        TTableFormatFileDesc tableFormat = new TTableFormatFileDesc();
        List<String> expected = Arrays.asList("oss://bkt/db/tbl/index/deletion-1.bin");

        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.getDeleteFiles(tableFormat)).thenReturn(expected);

        PluginDrivenScanNode node = nodeWithProvider(provider);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setTableFormatParams(tableFormat);

        List<String> result = node.getDeleteFiles(rangeDesc);

        Assertions.assertEquals(expected, result);
        Mockito.verify(provider).getDeleteFiles(tableFormat);
    }

    @Test
    public void rangeWithoutTableFormatParamsReturnsEmptyAndSkipsProvider() {
        // WHY: a range with no table-format params (e.g. a non-paimon split path) must yield empty
        // WITHOUT consulting the provider — legacy PaimonScanNode.getDeleteFiles null-guards exactly
        // this so the VERBOSE loop never NPEs. MUTATION: dropping the isSetTableFormatParams guard
        // would call the provider with null -> here it would fail verifyNoInteractions.
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        PluginDrivenScanNode node = nodeWithProvider(provider);

        List<String> result = node.getDeleteFiles(new TFileRangeDesc());

        Assertions.assertTrue(result.isEmpty());
        Mockito.verifyNoInteractions(provider);
    }

    @Test
    public void nullRangeReturnsEmpty() {
        // Defensive: a null range must not NPE (returns empty), mirroring the legacy guard.
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        PluginDrivenScanNode node = nodeWithProvider(provider);

        Assertions.assertTrue(node.getDeleteFiles(null).isEmpty());
        Mockito.verifyNoInteractions(provider);
    }

    @Test
    public void nullProviderReturnsEmpty() {
        // A connector without a scan plan provider (no scan capability) must yield empty, never NPE.
        PluginDrivenScanNode node = nodeWithProvider(null);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setTableFormatParams(new TTableFormatFileDesc());

        Assertions.assertTrue(node.getDeleteFiles(rangeDesc).isEmpty());
    }
}

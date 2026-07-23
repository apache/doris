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

package org.apache.doris.qe.cache;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Unit tests for the connector-agnostic scan-node recognition the SQL-result-cache migration added to
 * {@link CacheAnalyzer} after the hms SPI cutover. A flipped lakehouse table is a
 * {@code PluginDrivenMvccExternalTable} scanned by a {@link PluginDrivenScanNode}; the old gate matched
 * {@code instanceof HiveScanNode}, which both missed the plugin node AND (via the class hierarchy) would
 * wrongly include a jdbc-query TVF. The new gate keys on the TARGET TABLE's {@code MTMVRelatedTableIf}
 * capability, so it recognizes any lakehouse plugin table and excludes token-less nodes.
 *
 * <p>Tests the two new private members directly (via {@link Deencapsulation}) to avoid the full
 * analyze/MetricRepo bootstrap — the same reason the legacy {@code HmsQueryCacheTest} needed a real
 * catalog and is now disabled for the plugin path.</p>
 */
public class PluginTableCacheAnalyzerTest {

    private CacheAnalyzer analyzer;

    @BeforeEach
    public void setUp() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getSessionVariable()).thenReturn(new SessionVariable());
        // Unit-test constructor: scanNodes are supplied per-test via Deencapsulation.invoke on the helper.
        analyzer = new CacheAnalyzer(ctx, null, Collections.emptyList());
    }

    private PluginDrivenScanNode mockPluginScanNode(TableIf table, long selectedPartitionNum) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        desc.setTable(table);
        Mockito.when(node.getTupleDesc()).thenReturn(desc);
        Mockito.when(node.getSelectedPartitionNum()).thenReturn(selectedPartitionNum);
        return node;
    }

    /**
     * A flipped lakehouse plugin table (implements MTMVRelatedTableIf) IS recognized as cacheable. RED on
     * the pre-cutover HEAD, whose gate was {@code instanceof HiveScanNode} and never matched the plugin node.
     */
    @Test
    public void testRecognizePluginTableByCapability() {
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenScanNode node = mockPluginScanNode(table, 3L);
        boolean recognized = Deencapsulation.invoke(analyzer, "isExternalCacheableScanNode", node);
        Assert.assertTrue("a PluginDrivenMvccExternalTable scan must be cacheable", recognized);
    }

    /**
     * A jdbc-query TVF also emits a PluginDrivenScanNode, but its backing table is a FunctionGenTable with no
     * data-version token — the capability gate must EXCLUDE it (a class-based {@code instanceof
     * PluginDrivenScanNode} gate would have wrongly admitted it).
     */
    @Test
    public void testRejectTvfBackedNode() {
        FunctionGenTable tvfTable = Mockito.mock(FunctionGenTable.class);
        PluginDrivenScanNode node = mockPluginScanNode(tvfTable, 0L);
        boolean recognized = Deencapsulation.invoke(analyzer, "isExternalCacheableScanNode", node);
        Assert.assertFalse("a jdbc-query TVF (FunctionGenTable) has no token and must not be cacheable",
                recognized);
    }

    /** A scan node with no tuple descriptor is defensively excluded (no NPE). */
    @Test
    public void testRejectNullTupleDesc() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class);
        Mockito.when(node.getTupleDesc()).thenReturn(null);
        boolean recognized = Deencapsulation.invoke(analyzer, "isExternalCacheableScanNode", node);
        Assert.assertFalse(recognized);
    }

    /**
     * The cache-freshness marker (latestPartitionTime) is sourced from the connector's stable data-version
     * token {@code getNewestUpdateVersionOrTime()}, NOT the wall-clock {@code getUpdateTime()} that a flipped
     * plugin table would inherit (which would serve stale results). partitionNum comes from the scan node.
     */
    @Test
    public void testTokenSourcedFromConnectorFreshness() {
        long token = 1_700_000_000_000L;
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("hms_ctl");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("hms_db");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getNewestUpdateVersionOrTime()).thenReturn(token);

        PluginDrivenScanNode node = mockPluginScanNode(table, 5L);
        CacheAnalyzer.CacheTable cacheTable =
                Deencapsulation.invoke(analyzer, "buildCacheTableForExternalScanNode", node);

        Assert.assertSame(table, cacheTable.table);
        Assert.assertEquals(token, cacheTable.latestPartitionTime);
        Assert.assertEquals(5L, cacheTable.partitionNum);
    }

    /**
     * The quiet-window gate value ({@code latestPartitionUpdateMillis}) must come from the connector's
     * wall-clock accessor {@code getNewestUpdateTimeMillisForCache()} (a genuine epoch-millis), while the BE
     * PCache version key ({@code latestPartitionTime}) stays the raw data-version token. Conflating them is
     * what kept iceberg out of SqlCache: its token is MICROSECONDS, so subtracting it from a wall-clock now in
     * the 30s gate is always ~0 and never passes. RED if the gate value is sourced from the token.
     */
    @Test
    public void testGateValueSourcedFromWallClockAccessor() {
        long token = 1_700_000_000_000_000L;       // micros (iceberg-style token / version key)
        long wallClockMillis = 1_700_000_000_000L;  // millis (connector-normalized gate value)
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("iceberg_ctl");
        Mockito.when(db.getCatalog()).thenReturn(catalog);
        Mockito.when(db.getFullName()).thenReturn("iceberg_db");
        Mockito.when(table.getDatabase()).thenReturn(db);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getNewestUpdateVersionOrTime()).thenReturn(token);
        Mockito.when(table.getNewestUpdateTimeMillisForCache()).thenReturn(wallClockMillis);

        PluginDrivenScanNode node = mockPluginScanNode(table, 5L);
        CacheAnalyzer.CacheTable cacheTable =
                Deencapsulation.invoke(analyzer, "buildCacheTableForExternalScanNode", node);

        Assert.assertEquals("BE PCache version key stays the raw token", token, cacheTable.latestPartitionTime);
        Assert.assertEquals("the quiet-window gate value is the wall-clock millis", wallClockMillis,
                cacheTable.latestPartitionUpdateMillis);
    }
}

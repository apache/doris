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

package org.apache.doris.catalog;

import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TBinlogConfig;
import org.apache.doris.thrift.TBinlogFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Pins the binlog config every table kind ships to BE and Meta Service. CCR and changelog share the wire
 * struct, and BE / MS tell a changelog tablet by binlog_format = CHANGELOG (1), so the values must stay what
 * they were before changelog got its own properties.
 */
public class TabletBinlogConfigTest {
    @Test
    public void formatValuesAreTheWireValues() {
        Assertions.assertEquals(0, TBinlogFormat.STATEMENT_AND_SNAPSHOT.getValue());
        Assertions.assertEquals(1, TBinlogFormat.CHANGELOG.getValue());
        Assertions.assertEquals(0, OlapFile.BinlogFormatPB.STATEMENT_AND_SNAPSHOT.getNumber());
        Assertions.assertEquals(1, OlapFile.BinlogFormatPB.CHANGELOG.getNumber());
    }

    @Test
    public void plainTableShipsDisabledSnapshotFormat() {
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(), new ChangelogConfig());
        assertWire(wire, false, BinlogConfig.TTL_SECONDS, BinlogConfig.MAX_BYTES, BinlogConfig.MAX_HISTORY_NUMS,
                false, false, 0);
    }

    @Test
    public void ccrTableShipsItsBinlogConfig() {
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(true, 3600, 1024, 10),
                new ChangelogConfig());
        assertWire(wire, true, 3600, 1024, 10, false, false, 0);
    }

    @Test
    public void changelogTableShipsItsChangelogConfig() {
        ChangelogConfig changelogConfig = new ChangelogConfig(true, 600, true);
        changelogConfig.setConfigVersion(5);
        // The CCR retention of a changelog table never reaches its tablets.
        TabletBinlogConfig wire = TabletBinlogConfig.of(new BinlogConfig(false, 3600, 1024, 10), changelogConfig);
        assertWire(wire, true, 600, 1024, 10, true, true, 5);

        TabletBinlogConfig withoutBeforeImages = TabletBinlogConfig.of(new BinlogConfig(),
                new ChangelogConfig(true, ChangelogConfig.TTL_SECONDS, false));
        assertWire(withoutBeforeImages, true, ChangelogConfig.TTL_SECONDS, BinlogConfig.MAX_BYTES,
                BinlogConfig.MAX_HISTORY_NUMS, true, false, 0);
    }

    @Test
    public void tableShipsTheConfigOfItsKind() {
        OlapTable plain = newTable();
        Assertions.assertEquals(TabletBinlogConfig.of(new BinlogConfig(), new ChangelogConfig()),
                plain.getTabletBinlogConfig());

        OlapTable ccr = newTable();
        ccr.setBinlogConfig(new BinlogConfig(true, 3600, 1024, 10));
        Assertions.assertEquals(TabletBinlogConfig.of(new BinlogConfig(true, 3600, 1024, 10),
                new ChangelogConfig()), ccr.getTabletBinlogConfig());

        OlapTable changelog = newTable();
        changelog.setBinlogConfig(new BinlogConfig());
        changelog.setChangelogConfig(new ChangelogConfig(true, 600, false));
        TabletBinlogConfig wire = changelog.getTabletBinlogConfig();
        Assertions.assertEquals(TabletBinlogConfig.of(new BinlogConfig(), new ChangelogConfig(true, 600, false)),
                wire);
        Assertions.assertEquals(TBinlogFormat.CHANGELOG, wire.toThrift().getBinlogFormat());
        Assertions.assertNotEquals(ccr.getTabletBinlogConfig(), wire);
    }

    private static OlapTable newTable() {
        OlapTable table = new OlapTable();
        table.setTableProperty(new TableProperty(new HashMap<>()));
        return table;
    }

    private static void assertWire(TabletBinlogConfig wire, boolean enable, long ttlSeconds, long maxBytes,
            long maxHistoryNums, boolean changelog, boolean needHistoricalValue, long configVersion) {
        TBinlogConfig thrift = wire.toThrift();
        Assertions.assertTrue(thrift.isSetEnable() && thrift.isSetTtlSeconds() && thrift.isSetMaxBytes()
                && thrift.isSetMaxHistoryNums() && thrift.isSetBinlogFormat() && thrift.isSetNeedHistoricalValue()
                && thrift.isSetConfigVersion(), thrift.toString());
        Assertions.assertEquals(enable, thrift.isEnable());
        Assertions.assertEquals(ttlSeconds, thrift.getTtlSeconds());
        Assertions.assertEquals(maxBytes, thrift.getMaxBytes());
        Assertions.assertEquals(maxHistoryNums, thrift.getMaxHistoryNums());
        Assertions.assertEquals(changelog ? TBinlogFormat.CHANGELOG : TBinlogFormat.STATEMENT_AND_SNAPSHOT,
                thrift.getBinlogFormat());
        Assertions.assertEquals(needHistoricalValue, thrift.isNeedHistoricalValue());
        Assertions.assertEquals(configVersion, thrift.getConfigVersion());

        OlapFile.BinlogConfigPB protobuf = wire.toProtobuf();
        Assertions.assertTrue(protobuf.hasEnable() && protobuf.hasTtlSeconds() && protobuf.hasMaxBytes()
                && protobuf.hasMaxHistoryNums() && protobuf.hasBinlogFormat() && protobuf.hasNeedHistoricalValue()
                && protobuf.hasConfigVersion(), protobuf.toString());
        Assertions.assertEquals(enable, protobuf.getEnable());
        Assertions.assertEquals(ttlSeconds, protobuf.getTtlSeconds());
        Assertions.assertEquals(maxBytes, protobuf.getMaxBytes());
        Assertions.assertEquals(maxHistoryNums, protobuf.getMaxHistoryNums());
        Assertions.assertEquals(changelog ? OlapFile.BinlogFormatPB.CHANGELOG
                : OlapFile.BinlogFormatPB.STATEMENT_AND_SNAPSHOT, protobuf.getBinlogFormat());
        Assertions.assertEquals(needHistoricalValue, protobuf.getNeedHistoricalValue());
        Assertions.assertEquals(configVersion, protobuf.getConfigVersion());
    }
}

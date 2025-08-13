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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BlackholeResultHandler {
    private static final Logger LOG = LogManager.getLogger(BlackholeResultHandler.class);

    public static class BeBlackholeAggregatedData {
        private long scanRows = 0;
        private long scanBytes = 0;
        private long scanBytesFromLocalStorage = 0;
        private long scanBytesFromRemoteStorage = 0;

        // Get the last aggregated data value.
        public void processData(long scanRows, long scanBytes,
                long scanBytesFromLocalStorage, long scanBytesFromRemoteStorage) {
            this.scanRows = scanRows;
            this.scanBytes = scanBytes;
            this.scanBytesFromLocalStorage = scanBytesFromLocalStorage;
            this.scanBytesFromRemoteStorage = scanBytesFromRemoteStorage;
        }

        public long getScanRows() {
            return scanRows;
        }

        public long getScanBytes() {
            return scanBytes;
        }

        public long getScanBytesFromLocalStorage() {
            return scanBytesFromLocalStorage;
        }

        public long getScanBytesFromRemoteStorage() {
            return scanBytesFromRemoteStorage;
        }
    }

    private Map<String, BeBlackholeAggregatedData> blackholeBeDataMap = Maps.newHashMap();

    /**
     * Process blackhole data from BE nodes
     */
    public void processBlackholeData(Map<String, String> attachedInfos) {
        // Parse BE-specific data from attached infos
        String beId = attachedInfos.getOrDefault("be_id", "unknown");
        String scanRows = attachedInfos.getOrDefault("ScanRows", "0");
        String scanBytes = attachedInfos.getOrDefault("ScanBytes", "0");
        String localStorage = attachedInfos.getOrDefault("ScanBytesFromLocalStorage", "0");
        String remoteStorage = attachedInfos.getOrDefault("ScanBytesFromRemoteStorage", "0");

        BeBlackholeAggregatedData data = blackholeBeDataMap.computeIfAbsent(beId,
                k -> new BeBlackholeAggregatedData());
        try {
            data.processData(
                    Long.parseLong(scanRows),
                    Long.parseLong(scanBytes),
                    Long.parseLong(localStorage),
                    Long.parseLong(remoteStorage)
            );
        } catch (NumberFormatException e) {
            LOG.warn("Failed to parse BE data for BE {}: {}", beId, e.getMessage());
        }
    }

    /**
     * Send aggregated blackhole query results to the client
     */
    public void sendAggregatedBlackholeResults(StmtExecutor executor) throws IOException {
        // Create a result set with aggregated data per BE
        ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("BackendId", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanRows", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytes", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromLocalStorage", ScalarType.createVarchar(20)))
                .addColumn(new Column("ScanBytesFromRemoteStorage", ScalarType.createVarchar(20)))
                .build();

        List<List<String>> rows = Lists.newArrayList();
        long totalScanRows = 0;
        long totalScanBytes = 0;
        long totalScanBytesFromLocalStorage = 0;
        long totalScanBytesFromRemoteStorage = 0;

        // Add a row for each BE with its aggregated data
        for (Map.Entry<String, BeBlackholeAggregatedData> entry : blackholeBeDataMap.entrySet()) {
            String beId = entry.getKey();
            BeBlackholeAggregatedData data = entry.getValue();

            List<String> row = Lists.newArrayList(
                    beId,
                    String.valueOf(data.getScanRows()),
                    String.valueOf(data.getScanBytes()),
                    String.valueOf(data.getScanBytesFromLocalStorage()),
                    String.valueOf(data.getScanBytesFromRemoteStorage())
            );

            rows.add(row);

            // Accumulate totals
            totalScanRows += data.getScanRows();
            totalScanBytes += data.getScanBytes();
            totalScanBytesFromLocalStorage += data.getScanBytesFromLocalStorage();
            totalScanBytesFromRemoteStorage += data.getScanBytesFromRemoteStorage();
        }

        // Add a total row
        List<String> totalRow = Lists.newArrayList(
                "TOTAL",
                String.valueOf(totalScanRows),
                String.valueOf(totalScanBytes),
                String.valueOf(totalScanBytesFromLocalStorage),
                String.valueOf(totalScanBytesFromRemoteStorage)
        );
        rows.add(totalRow);

        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        executor.sendResultSet(resultSet);

        // Clear the map for the next query
        blackholeBeDataMap.clear();
    }

    public boolean hasData() {
        return !blackholeBeDataMap.isEmpty();
    }
}

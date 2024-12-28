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

package org.apache.doris.datasource.trinoconnector.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

import com.google.common.collect.Maps;
import io.trino.connector.ConnectorName;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TrinoConnectorSplit extends FileSplit {
    private static final Logger LOG = LogManager.getLogger(TrinoConnectorSplit.class);
    private static final LocationPath DUMMY_PATH = new LocationPath("/dummyPath", Maps.newHashMap());
    private ConnectorSplit connectorSplit;
    private TableFormatType tableFormatType;
    private final ConnectorName connectorName;

    public TrinoConnectorSplit(ConnectorSplit connectorSplit, ConnectorName connectorName) {
        super(DUMMY_PATH, 0, 0, 0, 0, null, null);
        this.connectorSplit = connectorSplit;
        this.tableFormatType = TableFormatType.TRINO_CONNECTOR;
        this.connectorName = connectorName;
        initSplitInfo();
    }

    public ConnectorSplit getSplit() {
        return connectorSplit;
    }

    public void setSplit(ConnectorSplit connectorSplit) {
        this.connectorSplit = connectorSplit;
    }

    public TableFormatType getTableFormatType() {
        return tableFormatType;
    }

    public void setTableFormatType(TableFormatType tableFormatType) {
        this.tableFormatType = tableFormatType;
    }

    private void initSplitInfo() {
        // set hosts
        List<HostAddress> addresses = connectorSplit.getAddresses();
        this.hosts = new String[addresses.size()];
        for (int i = 0; i < addresses.size(); i++) {
            hosts[i] = addresses.get(0).getHostText();
        }

        switch (connectorName.toString()) {
            case "hive":
                initHiveSplitInfo();
                break;
            default:
                LOG.debug("Unknow connector name: " + connectorName);
                return;
        }
    }

    private void initHiveSplitInfo() {
        Object info = connectorSplit.getInfo();
        if (info instanceof Map) {
            Map<String, Object> splitInfo = (Map<String, Object>) info;
            path = new LocationPath((String) splitInfo.getOrDefault("path", "dummyPath"), Maps.newHashMap());
            start = (long) splitInfo.getOrDefault("start", 0);
            length = (long) splitInfo.getOrDefault("length", 0);
            fileLength  = (long) splitInfo.getOrDefault("estimatedFileSize", 0);
            partitionValues = new ArrayList<>();
            partitionValues.add((String) splitInfo.getOrDefault("partitionName", ""));
        }
    }
}

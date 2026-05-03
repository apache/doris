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

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.connector.api.scan.ConnectorScanRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link FileSplit} that wraps a {@link ConnectorScanRange} from the SPI layer.
 *
 * <p>Maps the connector scan range's data to the FileSplit interface so it can
 * flow through the existing {@code FileQueryScanNode} pipeline. The original
 * {@link ConnectorScanRange} is preserved and accessible for format-specific
 * parameter extraction in {@code PluginDrivenScanNode.setScanParams()}.</p>
 */
public class PluginDrivenSplit extends FileSplit {

    private final ConnectorScanRange connectorScanRange;

    public PluginDrivenSplit(ConnectorScanRange scanRange) {
        super(buildPath(scanRange),
                scanRange.getStart(),
                scanRange.getLength(),
                scanRange.getFileSize(),
                scanRange.getModificationTime(),
                scanRange.getHosts().toArray(new String[0]),
                buildPartitionValues(scanRange));
        this.connectorScanRange = scanRange;
    }

    /** Returns the underlying connector scan range for format-specific param extraction. */
    public ConnectorScanRange getConnectorScanRange() {
        return connectorScanRange;
    }

    @Override
    public Object getInfo() {
        return null;
    }

    @Override
    public String getPathString() {
        return connectorScanRange.getPath().orElse("connector://virtual");
    }

    private static LocationPath buildPath(ConnectorScanRange scanRange) {
        String pathStr = scanRange.getPath().orElse("connector://virtual");
        return LocationPath.of(pathStr);
    }

    private static List<String> buildPartitionValues(ConnectorScanRange scanRange) {
        Map<String, String> partValues = scanRange.getPartitionValues();
        if (partValues == null || partValues.isEmpty()) {
            return null;
        }
        return new ArrayList<>(partValues.values());
    }
}

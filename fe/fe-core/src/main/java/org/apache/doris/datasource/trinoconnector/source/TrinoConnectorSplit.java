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

import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.Path;

public class TrinoConnectorSplit extends FileSplit {
    private ConnectorSplit connectorSplit;
    private TableFormatType tableFormatType;

    public TrinoConnectorSplit(ConnectorSplit connectorSplit) {
        super(new Path("dummyPath"), 0, 0, 0, null, null);
        this.connectorSplit = connectorSplit;
        this.tableFormatType = TableFormatType.TRINO_CONNECTOR;
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

}

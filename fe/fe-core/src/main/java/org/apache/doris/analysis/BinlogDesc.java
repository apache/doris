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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.sync.DataSyncJobType;

import com.google.common.collect.Maps;

import java.util.Map;

// Binlog descriptor
//
// Example:
// FROM BINLOG
// (
//   "type" = "canal",
//   "canal.server.ip" = "127.0.0.1",
//   "canal.server.port" = "11111",
//   "canal.destination" = "example",
//   "canal.username" = "canal",
//   "canal.password" = "canal"
// )

public class BinlogDesc {
    private static final String TYPE = "type";
    private Map<String, String> properties;
    private DataSyncJobType dataSyncJobType;

    public BinlogDesc(Map<String, String> properties) {
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
        this.dataSyncJobType = DataSyncJobType.UNKNOWN;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public DataSyncJobType getDataSyncJobType() {
        return dataSyncJobType;
    }

    public void analyze() throws AnalysisException {
        if (!properties.containsKey(TYPE)) {
            throw new AnalysisException("Binlog properties must contain property `type`");
        }
        dataSyncJobType = DataSyncJobType.fromString(properties.get(TYPE));
    }
}

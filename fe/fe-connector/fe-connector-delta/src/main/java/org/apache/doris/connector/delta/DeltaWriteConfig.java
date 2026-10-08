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

package org.apache.doris.connector.delta;

import java.util.List;
import java.util.Map;

/** Validated plugin-internal file writer settings; never exposed through the shared SPI. */
final class DeltaWriteConfig {
    private final String writeLocation;
    private final List<String> partitionColumns;
    private final Map<String, String> properties;

    DeltaWriteConfig(String writeLocation, List<String> partitionColumns, Map<String, String> properties) {
        this.writeLocation = writeLocation;
        this.partitionColumns = List.copyOf(partitionColumns);
        this.properties = Map.copyOf(properties);
    }

    String getWriteLocation() {
        return writeLocation;
    }

    String getFileFormat() {
        return "parquet";
    }

    List<String> getPartitionColumns() {
        return partitionColumns;
    }

    Map<String, String> getProperties() {
        return properties;
    }
}

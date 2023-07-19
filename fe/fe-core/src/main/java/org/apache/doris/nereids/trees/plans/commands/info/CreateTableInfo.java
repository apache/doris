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

package org.apache.doris.nereids.trees.plans.commands.info;

import java.util.List;

/**
 * table info in creating table.
 */
public class CreateTableInfo {
    private final String dbName;
    private final String tableName;
    private final List<ColumnDef> columns;
    private final List<IndexDef> indexes;
    private final String engineName;
    // private final KeysType keyType;
    private final List<String> keys;
    private final String comment;
    // private final List<PartitionInfo> partitions;
    private final DistributionDesc distribution;
    private final List<RollupDef> rollups;
    private final Map<String, String> properties;
    
    public CreateTableInfo(String dbName, String tableName, List<ColumnDef> columns, List<IndexDef> indexes,
            String engineName, List<String> keys, String comment, DistributionDesc distribution,
            List<RollupDef> rollups, Map<String, String> properties) {
    }
}

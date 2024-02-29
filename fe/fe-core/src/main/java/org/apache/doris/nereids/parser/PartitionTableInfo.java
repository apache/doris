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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;

import java.util.List;

/**
 * partition info for 'PARTITION BY'
 */
public class PartitionTableInfo {

    public static final PartitionTableInfo EMPTY = new PartitionTableInfo(
            false,
            null,
            "",
            null,
            null);

    private boolean isAutoPartition;
    private List<Expression> autoPartitionExprs;
    private String partitionType;
    private List<String> partitionColumns;
    private List<PartitionDefinition> partitions;

    public PartitionTableInfo(
            boolean isAutoPartition,
            List<Expression> autoPartitionExprs,
            String partitionType,
            List<String> partitionColumns,
            List<PartitionDefinition> partitions) {
        this.isAutoPartition = isAutoPartition;
        this.autoPartitionExprs = autoPartitionExprs;
        this.partitionType = partitionType;
        this.partitionColumns = partitionColumns;
        this.partitions = partitions;
    }

    public boolean isAutoPartition() {
        return isAutoPartition;
    }

    public List<Expression> getAutoPartitionExprs() {
        return autoPartitionExprs;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public List<PartitionDefinition> getPartitions() {
        return partitions;
    }
}

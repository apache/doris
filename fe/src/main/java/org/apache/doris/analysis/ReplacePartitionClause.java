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
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

// clause which is used to replace temporary partition
// eg:
// ALTER TABLE tbl REPLACE PARTITION (p1, p2, p3) WITH TEMPORARY PARTITION(tp1, tp2);
public class ReplacePartitionClause extends AlterTableClause {
    private List<String> partitionNames;
    private List<String> tempPartitionNames;
    private Map<String, String> properties = Maps.newHashMap();

    // "isStrictMode" is got from property "strict_range", and default is true.
    // If true, when replacing partition, the range of partitions must same as the range of temp partitions.
    private boolean isStrictRange;
    
    // "useTempPartitionName" is got from property "use_temp_partition_name", and default is false.
    // If false, after replacing, the replaced partition's name will remain unchanged.
    // Otherwise, the replaced partition's name will be the temp partitions name.
    // This parameter is valid only when the number of partitions is the same as the number of temp partitions.
    // For example:
    // 1. REPLACE PARTITION (p1, p2, p3) WITH TEMPORARY PARTITION(tp1, tp2) PROPERTIES("use_temp_partition_name" = "false");
    //      "use_temp_partition_name" will take no effect after replacing, and the partition names will be "tp1" and "tp2".
    //
    // 2. REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION(tp1, tp2) PROPERTIES("use_temp_partition_name" = "false");
    //      alter replacing, the partition names will be "p1" and "p2".
    //      but if "use_temp_partition_name" is true, the partition names will be "tp1" and "tp2".
    private boolean useTempPartitionName;

    public ReplacePartitionClause(List<String> partitionNames, List<String> tempPartitionNames,
            Map<String, String> properties) {
        this.partitionNames = partitionNames;
        this.tempPartitionNames = tempPartitionNames;
        this.needTableStable = false;
        this.properties = properties;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames;
    }

    public boolean isStrictRange() {
        return isStrictRange;
    }

    public boolean useTempPartitionName() {
        return useTempPartitionName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (partitionNames.isEmpty()) {
            throw new AnalysisException("No partition specified");
        }

        if (tempPartitionNames.isEmpty()) {
            throw new AnalysisException("No temp partition specified");
        }

        this.isStrictRange = PropertyAnalyzer.analyzeStrictRange(properties, true);
        this.useTempPartitionName = PropertyAnalyzer.analyzeUseTempPartitionName(properties, false);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE PARTITION(");
        sb.append(Joiner.on(", ").join(partitionNames)).append(")");
        sb.append(" WITH TEMPORARY PARTITION(");
        sb.append(Joiner.on(", ").join(tempPartitionNames)).append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

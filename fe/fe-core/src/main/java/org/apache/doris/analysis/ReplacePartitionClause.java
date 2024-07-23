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

import org.apache.doris.alter.AlterOpType;
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
    private PartitionNames partitionNames;
    private PartitionNames tempPartitionNames;
    private Map<String, String> properties = Maps.newHashMap();

    // "isStrictMode" is got from property "strict_range", and default is true.
    // If true, when replacing partition, the range of partitions must same as the range of temp partitions.
    private boolean isStrictRange;

    // "useTempPartitionName" is got from property "use_temp_partition_name", and default is false.
    // If false, after replacing, the replaced partition's name will remain unchanged.
    // Otherwise, the replaced partition's name will be the temp partitions name.
    // This parameter is valid only when the number of partitions is the same as the number of temp partitions.
    // For example:
    // 1. REPLACE PARTITION (p1, p2, p3) WITH TEMPORARY PARTITION(tp1, tp2)
    //    PROPERTIES("use_temp_partition_name" = "false");
    //      "use_temp_partition_name" will take no effect after replacing,
    //      and the partition names will be "tp1" and "tp2".
    //
    // 2. REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION(tp1, tp2) PROPERTIES("use_temp_partition_name" = "false");
    //      alter replacing, the partition names will be "p1" and "p2".
    //      but if "use_temp_partition_name" is true, the partition names will be "tp1" and "tp2".
    private boolean useTempPartitionName;

    // The replaced partitions will be moved to recycle bin when "forceDropNormalPartition" is false,
    // and instead, these partitions will be deleted directly.
    private boolean forceDropOldPartition;

    public ReplacePartitionClause(PartitionNames partitionNames, PartitionNames tempPartitionNames,
            boolean isForce, Map<String, String> properties) {
        super(AlterOpType.REPLACE_PARTITION);
        this.partitionNames = partitionNames;
        this.tempPartitionNames = tempPartitionNames;
        this.needTableStable = false;
        this.forceDropOldPartition = isForce;
        this.properties = properties;
    }

    public List<String> getPartitionNames() {
        return partitionNames.getPartitionNames();
    }

    public List<String> getTempPartitionNames() {
        return tempPartitionNames.getPartitionNames();
    }

    public boolean isStrictRange() {
        return isStrictRange;
    }

    public boolean useTempPartitionName() {
        return useTempPartitionName;
    }

    public boolean isForceDropOldPartition() {
        return forceDropOldPartition;
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (partitionNames == null || tempPartitionNames == null) {
            throw new AnalysisException("No partition specified");
        }

        partitionNames.analyze(analyzer);
        tempPartitionNames.analyze(analyzer);

        if (partitionNames.isTemp() || !tempPartitionNames.isTemp()) {
            throw new AnalysisException("Only support replace partitions with temp partitions");
        }

        this.isStrictRange = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true);
        this.useTempPartitionName = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REPLACE PARTITION(");
        sb.append(Joiner.on(", ").join(partitionNames.getPartitionNames())).append(")");
        sb.append(" WITH TEMPORARY PARTITION(");
        sb.append(Joiner.on(", ").join(tempPartitionNames.getPartitionNames())).append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

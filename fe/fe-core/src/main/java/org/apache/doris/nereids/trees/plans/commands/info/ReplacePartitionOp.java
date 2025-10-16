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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.info.PartitionNamesInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ReplacePartitionOp
 */
public class ReplacePartitionOp extends AlterTableOp {
    private PartitionNamesInfo partitionNames;
    private PartitionNamesInfo tempPartitionNames;
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

    // Expected versions for partition version checking
    // Format: "partition_name:expected_version"
    // Example: "p20240601:2, p20240602:2"
    private Map<String, Long> expectedVersions;

    // The replaced partitions will be moved to recycle bin when "forceDropNormalPartition" is false,
    // and instead, these partitions will be deleted directly.
    private boolean forceDropOldPartition;

    /**
     * ReplacePartitionOp
     */
    public ReplacePartitionOp(PartitionNamesInfo partitionNames, PartitionNamesInfo tempPartitionNames,
            boolean isForce, Map<String, String> properties) {
        super(AlterOpType.REPLACE_PARTITION);
        this.partitionNames = partitionNames;
        this.tempPartitionNames = tempPartitionNames;
        this.needTableStable = false;
        this.forceDropOldPartition = isForce;
        this.properties = properties;

        // ATTN: During ReplacePartitionClause.analyze(), the default value of isStrictRange is true.
        // However, ReplacePartitionClause instances constructed by internal code do not call analyze(),
        // so their isStrictRange value is incorrect (e.g., INSERT INTO ... OVERWRITE).
        //
        // Considering this, we should handle the relevant properties when constructing.
        this.isStrictRange = getBoolProperty(properties, PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true);
        this.useTempPartitionName = getBoolProperty(
                properties, PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false);
        this.expectedVersions = new HashMap<>();
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

    public Map<String, Long> getExpectedVersions() {
        return expectedVersions;
    }

    public boolean hasVersionChecking() {
        return expectedVersions != null && !expectedVersions.isEmpty();
    }

    public boolean isForceDropOldPartition() {
        return forceDropOldPartition;
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (partitionNames == null || tempPartitionNames == null) {
            throw new AnalysisException("No partition specified");
        }

        partitionNames.validate();
        tempPartitionNames.validate();

        if (partitionNames.isTemp() || !tempPartitionNames.isTemp()) {
            throw new AnalysisException("Only support replace partitions with temp partitions");
        }

        this.isStrictRange = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_STRICT_RANGE, true);
        this.useTempPartitionName = PropertyAnalyzer.analyzeBooleanProp(properties,
                PropertyAnalyzer.PROPERTIES_USE_TEMP_PARTITION_NAME, false);

        // Parse expected versions for partition version checking
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_EXPECTED_VERSIONS)) {
            String expectedVersionsStr = properties.get(PropertyAnalyzer.PROPERTIES_EXPECTED_VERSIONS);
            parseExpectedVersions(expectedVersionsStr);
            properties.remove(PropertyAnalyzer.PROPERTIES_EXPECTED_VERSIONS);
            // Validate that all partitions in expected_versions match the partitions to be replaced
            validateExpectedVersionsPartitions();
        }

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new ReplacePartitionClause(partitionNames, tempPartitionNames,
                forceDropOldPartition, properties, isStrictRange, useTempPartitionName, expectedVersions);
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

    public static boolean getBoolProperty(Map<String, String> properties, String propKey, boolean defaultVal) {
        if (properties != null && properties.containsKey(propKey)) {
            String val = properties.get(propKey);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    /**
     * Validate that all partitions in expected_versions match the partitions to be replaced.
     */
    private void validateExpectedVersionsPartitions() throws AnalysisException {
        if (expectedVersions == null || expectedVersions.isEmpty()) {
            return;
        }

        Set<String> partitionsToReplaceSet = new HashSet<>(getPartitionNames());
        Set<String> expectedPartitionsSet = expectedVersions.keySet();

        if (!partitionsToReplaceSet.equals(expectedPartitionsSet)) {
            throw new AnalysisException(
                    "Partitions in expected_versions must exactly match the partitions to be replaced. "
                            + "Expected: " + partitionsToReplaceSet + ", but got: " + expectedPartitionsSet);
        }
    }

    /**
     * Parse expected versions from string format.
     * Format: "partition_name1:version1, partition_name2:version2"
     * Example: "p20240601:2, p20240602:3"
     */
    private void parseExpectedVersions(String expectedVersionsStr) throws AnalysisException {
        if (expectedVersionsStr == null || expectedVersionsStr.trim().isEmpty()) {
            return;
        }

        String[] pairs = expectedVersionsStr.split(",");
        for (String pair : pairs) {
            String trimmedPair = pair.trim();
            if (trimmedPair.isEmpty()) {
                continue;
            }

            String[] parts = trimmedPair.split(":");
            if (parts.length != 2) {
                throw new AnalysisException(
                        "Invalid expected_versions format. Expected 'partition_name:version', got: "
                                + trimmedPair);
            }

            String partitionName = parts[0].trim();
            String versionStr = parts[1].trim();

            if (partitionName.isEmpty()) {
                throw new AnalysisException("Partition name cannot be empty in expected_versions");
            }

            try {
                long version = Long.parseLong(versionStr);
                if (version <= 0) {
                    throw new AnalysisException("Partition version must be positive, got: " + version);
                }
                expectedVersions.put(partitionName, version);
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid version number: " + versionStr);
            }
        }
    }
}

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
import org.apache.doris.analysis.ModifyPartitionClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * ModifyPartitionOp
 */
public class ModifyPartitionOp extends AlterTableOp {
    private List<String> partitionNames;
    private Map<String, String> properties = Maps.newHashMap();
    private boolean isTempPartition;
    private boolean needExpand;

    /**
     * ModifyPartitionOp
     */
    public ModifyPartitionOp(List<String> partitionNames, Map<String, String> properties,
            boolean isTempPartition) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = partitionNames;
        if (properties != null) {
            this.properties.putAll(properties);
        }
        this.needExpand = false;
        // ATTN: currently, modify partition only allow 3 kinds of operations:
        // 1. modify replication num
        // 2. modify data property
        // 3. modify in memory
        // And these 3 operations does not require table to be stable.
        // If other kinds of operations be added later, "needTableStable" may be changed.
        this.needTableStable = false;
        this.isTempPartition = isTempPartition;
    }

    // c'tor for 'Modify Partition(*)' clause
    private ModifyPartitionOp(Map<String, String> properties, boolean isTempPartition) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = Lists.newArrayList();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        this.needExpand = true;
        this.needTableStable = false;
        this.isTempPartition = isTempPartition;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public static ModifyPartitionOp createStarClause(Map<String, String> properties,
            boolean isTempPartition) {
        return new ModifyPartitionOp(properties, isTempPartition);
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (partitionNames == null || (!needExpand && partitionNames.isEmpty())) {
            throw new AnalysisException("Partition names is not set or empty");
        }

        if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
            throw new AnalysisException("there are empty partition name");
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }

        // check properties here
        checkProperties(Maps.newHashMap(properties));
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new ModifyPartitionClause(partitionNames, properties, isTempPartition, needExpand);
    }

    // Check the following properties' legality before modifying partition.
    // 1. replication_num or replication_allocation
    // 2. storage_medium && storage_cooldown_time
    // 3. in_memory
    // 4. tablet type
    private void checkProperties(Map<String, String> properties) throws AnalysisException {
        // 1. replica allocation
        PropertyAnalyzer.analyzeReplicaAllocation(properties, "");

        // 2. in memory
        boolean isInMemory = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY,
                false);
        if (isInMemory == true) {
            throw new AnalysisException("Not support set 'in_memory'='true' now!");
        }

        // 3. tablet type
        PropertyAnalyzer.analyzeTabletType(properties);

        // 4. mutable
        PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_MUTABLE, true);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public boolean isTempPartition() {
        return isTempPartition;
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
        sb.append("MODIFY PARTITION ");
        if (isTempPartition) {
            sb.append("TEMPORARY ");
        }
        sb.append("(");
        if (needExpand) {
            sb.append("*");
        } else {
            sb.append(Joiner.on(", ").join(partitionNames));
        }
        sb.append(")");
        sb.append(" SET (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

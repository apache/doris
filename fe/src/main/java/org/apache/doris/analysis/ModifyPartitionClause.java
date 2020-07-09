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
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

// clause which is used to modify partition properties
// 1. Modify Partition p1 set ("replication_num" = "3")
// 2. Modify Partition (p1, p3, p4) set ("replication_num" = "3")
// 3. Modify Partition (*) set ("replication_num" = "3")
public class ModifyPartitionClause extends AlterTableClause {

    private List<String> partitionNames;
    private Map<String, String> properties;
    private boolean needExpand = false;

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    // c'tor for non-star clause
    public ModifyPartitionClause(List<String> partitionNames, Map<String, String> properties) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = partitionNames;
        this.properties = properties;
        this.needExpand = false;
        // ATTN: currently, modify partition only allow 3 kinds of operations:
        // 1. modify replication num
        // 2. modify data property
        // 3. modify in memory
        // And these 3 operations does not require table to be stable.
        // If other kinds of operations be added later, "needTableStable" may be changed.
        this.needTableStable = false;
    }

    // c'tor for 'Modify Partition(*)' clause
    private ModifyPartitionClause(Map<String, String> properties) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = Lists.newArrayList();
        this.properties = properties;
        this.needExpand = true;
        this.needTableStable = false;
    }

    public static ModifyPartitionClause createStarClause(Map<String, String> properties) {
        return new ModifyPartitionClause(properties);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
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

    // Check the following properties' legality before modifying partition.
    // 1. replication_num
    // 2. storage_medium && storage_cooldown_time
    // 3. in_memory
    // 4. tablet type
    private void checkProperties(Map<String, String> properties) throws AnalysisException {
        // 1. data property
        DataProperty newDataProperty = null;
        newDataProperty = PropertyAnalyzer.analyzeDataProperty(properties, DataProperty.DEFAULT_DATA_PROPERTY);
        Preconditions.checkNotNull(newDataProperty);

        // 2. replication num
        short newReplicationNum = (short) -1;
        newReplicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);
        Preconditions.checkState(newReplicationNum != (short) -1);

        // 3. in memory
        PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // 4. tablet type
        PropertyAnalyzer.analyzeTabletType(properties);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public boolean isNeedExpand() {
        return this.needExpand;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY PARTITION ");
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

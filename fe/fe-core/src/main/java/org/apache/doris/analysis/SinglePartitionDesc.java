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

import org.apache.doris.analysis.PartitionKeyDesc.PartitionKeyValueType;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.FixedRangePartition;
import org.apache.doris.nereids.trees.plans.commands.info.InPartition;
import org.apache.doris.nereids.trees.plans.commands.info.LessThanPartition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SinglePartitionDesc implements AllPartitionDesc {
    private boolean isAnalyzed;

    private boolean ifNotExists;

    private String partName;
    private PartitionKeyDesc partitionKeyDesc;
    private Map<String, String> properties;

    private DataProperty partitionDataProperty;
    private ReplicaAllocation replicaAlloc;
    private boolean isInMemory = false;
    private TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
    private Long versionInfo;
    private String storagePolicy;
    private boolean isMutable;

    public SinglePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                               Map<String, String> properties) {
        this.ifNotExists = ifNotExists;

        this.isAnalyzed = false;

        this.partName = partName;
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;

        this.partitionDataProperty = new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM);
        this.replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        this.storagePolicy = "";
    }

    /**
     * for Nereids
     */
    public SinglePartitionDesc(boolean ifNotExists, String partName,
            PartitionKeyDesc partitionKeyDesc, ReplicaAllocation replicaAlloc,
            Map<String, String> properties, DataProperty partitionDataProperty, boolean isInMemory,
            TTabletType tabletType, Long versionInfo, String storagePolicy, boolean isMutable) {
        this.isAnalyzed = true;
        this.ifNotExists = ifNotExists;
        this.partName = partName;
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;
        this.partitionDataProperty = partitionDataProperty;
        this.replicaAlloc = replicaAlloc;
        this.isInMemory = isInMemory;
        this.tabletType = tabletType;
        this.versionInfo = versionInfo;
        this.storagePolicy = storagePolicy;
        this.isMutable = isMutable;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getPartitionName() {
        return partName;
    }

    public PartitionKeyDesc getPartitionKeyDesc() {
        return partitionKeyDesc;
    }

    public DataProperty getPartitionDataProperty() {
        return partitionDataProperty;
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public void setReplicaAlloc(ReplicaAllocation replicaAlloc) {
        this.replicaAlloc = replicaAlloc;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean isMutable() {
        return isMutable;
    }

    public TTabletType getTabletType() {
        return tabletType;
    }

    public Long getVersionInfo() {
        return versionInfo;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void analyze(int partColNum, Map<String, String> otherProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        boolean hasStoragePolicy = false;
        if (properties != null) {
            hasStoragePolicy = properties.keySet().stream()
                    .anyMatch(iter -> {
                        boolean equal = iter.compareToIgnoreCase(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY) == 0;
                        // when find has storage policy properties, here will set it in partition
                        if (equal) {
                            storagePolicy = properties.get(iter);
                        }
                        return equal;
                    });
        }

        FeNameFormat.checkPartitionName(partName);

        partitionKeyDesc.analyze(partColNum);

        Map<String, String> mergedMap = Maps.newHashMap();
        // Should putAll `otherProperties` before `this.properties`,
        // because the priority of partition is higher than table
        if (otherProperties != null) {
            mergedMap.putAll(otherProperties);
        }
        if (this.properties != null) {
            mergedMap.putAll(this.properties);
        }
        this.properties = mergedMap;

        // analyze data property
        partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM));
        Preconditions.checkNotNull(partitionDataProperty);

        // analyze replication num
        replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
        if (replicaAlloc.isNotSet()) {
            replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        }

        // analyze version info
        versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);

        // analyze in memory
        isInMemory = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        if (isInMemory == true) {
            throw new AnalysisException("Not support set 'in_memory'='true' now!");
        }

        // analyze is mutable
        isMutable = PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_MUTABLE, true);

        tabletType = PropertyAnalyzer.analyzeTabletType(properties);

        if (otherProperties == null) {
            // check unknown properties
            if (properties != null && !properties.isEmpty()) {
                if (!hasStoragePolicy) {
                    MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
                    throw new AnalysisException("Unknown properties: " + mapJoiner.join(properties));
                }
            }
        }

        this.isAnalyzed = true;
    }

    public boolean isAnalyzed() {
        return this.isAnalyzed;
    }

    public void setAnalyzed(boolean analyzed) {
        isAnalyzed = analyzed;
    }

    public String getStoragePolicy() {
        return this.storagePolicy;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(partName);
        if (partitionKeyDesc.getPartitionType() == PartitionKeyValueType.LESS_THAN) {
            sb.append(" VALUES LESS THAN ");
        } else {
            sb.append(" VALUES ");
        }
        sb.append(partitionKeyDesc.toSql());

        if (properties != null && !properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * translate To PartitionDefinition
     */
    public PartitionDefinition translateToPartitionDefinition() {
        PartitionDefinition partitionDefinition;

        switch (partitionKeyDesc.getPartitionType()) {
            case IN:
                List<List<Expression>> values = partitionKeyDesc.getInValues().stream()
                        .map(innerLilst -> innerLilst.stream()
                            .map(partitionValue -> {
                                if (partitionValue.isNullPartition()) {
                                    return new NullLiteral();
                                } else if (partitionValue.isMax()) {
                                    return PartitionDefinition.MaxValue.INSTANCE;
                                } else {
                                    String stringValue = partitionValue.getStringValue();
                                    return Literal.of(stringValue);
                                }
                            })
                            .map(literal -> (Expression) literal)
                            .collect(Collectors.toList()))
                        .collect(Collectors.toList());

                partitionDefinition = new InPartition(ifNotExists, partName, values);
                break;
            case LESS_THAN:
                List<Expression> expressions = partitionKeyDesc.getUpperValues().stream()
                        .map(partitionValue -> {
                            if (partitionValue.isNullPartition()) {
                                return new NullLiteral();
                            } else if (partitionValue.isMax()) {
                                return PartitionDefinition.MaxValue.INSTANCE;
                            } else {
                                String stringValue = partitionValue.getStringValue();
                                return Literal.of(stringValue);
                            }
                        })
                        .map(literal -> (Expression) literal)
                        .collect(Collectors.toList());

                partitionDefinition = new LessThanPartition(ifNotExists, partName, expressions);
                break;
            case FIXED:
                List<Expression> lower = partitionKeyDesc.getLowerValues().stream()
                        .map(partitionValue -> {
                            if (partitionValue.isNullPartition()) {
                                return new NullLiteral();
                            } else if (partitionValue.isMax()) {
                                return PartitionDefinition.MaxValue.INSTANCE;
                            } else {
                                String stringValue = partitionValue.getStringValue();
                                return Literal.of(stringValue);
                            }
                        })
                        .map(literal -> (Expression) literal)
                        .collect(Collectors.toList());

                List<Expression> upper = partitionKeyDesc.getUpperValues().stream()
                        .map(partitionValue -> {
                            if (partitionValue.isNullPartition()) {
                                return new NullLiteral();
                            } else if (partitionValue.isMax()) {
                                return PartitionDefinition.MaxValue.INSTANCE;
                            } else {
                                String stringValue = partitionValue.getStringValue();
                                return Literal.of(stringValue);
                            }
                        })
                        .map(literal -> (Expression) literal)
                        .collect(Collectors.toList());

                partitionDefinition = new FixedRangePartition(ifNotExists, partName, lower, upper);
                break;
            default:
                return null;
        }

        partitionDefinition.setMutable(isMutable);
        partitionDefinition.setInMemory(isInMemory);
        partitionDefinition.setReplicaAllocation(replicaAlloc);
        partitionDefinition.setVersionInfo(versionInfo);
        partitionDefinition.setTabletType(tabletType);
        partitionDefinition.setProperties(properties);
        partitionDefinition.setPartitionDataProperty(partitionDataProperty);

        return partitionDefinition;
    }

}

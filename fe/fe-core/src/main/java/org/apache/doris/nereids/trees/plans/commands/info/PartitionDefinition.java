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

import org.apache.doris.analysis.AllPartitionDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TTabletType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * abstract class for partition definition
 */
public abstract class PartitionDefinition {

    protected boolean ifNotExists;
    protected String partitionName;
    protected Map<String, String> properties;

    protected List<DataType> partitionTypes;
    protected DataProperty partitionDataProperty =
            new DataProperty(DataProperty.DEFAULT_STORAGE_MEDIUM);
    protected ReplicaAllocation replicaAllocation = ReplicaAllocation.DEFAULT_ALLOCATION;
    protected boolean isInMemory = false;
    protected TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
    protected Long versionInfo;
    protected String storagePolicy = "";
    protected boolean isMutable;

    public PartitionDefinition(boolean ifNotExists, String partName) {
        this.ifNotExists = ifNotExists;
        this.partitionName = partName;
    }

    public PartitionDefinition withProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public abstract AllPartitionDesc translateToCatalogStyle();

    /**
     * Validate the properties.
     * Derived class can override this method to do more validation.
     */
    public void validate(Map<String, String> otherProperties) {
        try {
            if (PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(otherProperties)) {
                String storagePolicy = PropertyAnalyzer.analyzeStoragePolicy(properties);
                if (!storagePolicy.isEmpty()) {
                    throw new AnalysisException(
                            "Can not create UNIQUE KEY table that enables Merge-On-write"
                                    + " with storage policy(" + storagePolicy + ")");
                }
            }
            boolean hasStoragePolicy = false;
            if (properties != null) {
                hasStoragePolicy = properties.keySet().stream().anyMatch(iter -> {
                    boolean equal = iter
                            .compareToIgnoreCase(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY) == 0;
                    // when find has storage policy properties, here will set it in partition
                    if (equal) {
                        storagePolicy = properties.get(iter);
                    }
                    return equal;
                });
            }

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
            replicaAllocation = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
            if (replicaAllocation.isNotSet()) {
                replicaAllocation = ReplicaAllocation.DEFAULT_ALLOCATION;
            }
            // analyze version info
            versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);

            // analyze in memory
            isInMemory = PropertyAnalyzer.analyzeBooleanProp(properties,
                    PropertyAnalyzer.PROPERTIES_INMEMORY, false);
            if (isInMemory == true) {
                throw new AnalysisException("Not support set 'in_memory'='true' now!");
            }

            // analyze is mutable
            isMutable = PropertyAnalyzer.analyzeBooleanProp(properties,
                    PropertyAnalyzer.PROPERTIES_MUTABLE, true);

            tabletType = PropertyAnalyzer.analyzeTabletType(properties);

            if (otherProperties == null) {
                // check unknown properties
                if (properties != null && !properties.isEmpty()) {
                    if (!hasStoragePolicy) {
                        Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
                        throw new AnalysisException(
                                "Unknown properties: " + mapJoiner.join(properties));
                    }
                }
            }
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e.getCause());
        }
    }

    /**
     * get partition name
     */
    public String getPartitionName() {
        throw new UnsupportedOperationException("Should not get partition name from step partition");
    }

    public void setPartitionTypes(List<DataType> partitionTypes) {
        this.partitionTypes = partitionTypes;
    }

    /**
     * translate partition value.
     */
    protected PartitionValue toLegacyPartitionValueStmt(Expression e) {
        if (e.isLiteral()) {
            return new PartitionValue(((Literal) e).getStringValue(), e.isNullLiteral());
        } else if (e instanceof MaxValue) {
            return PartitionValue.MAX_VALUE;
        }
        throw new AnalysisException("Unsupported partition value");
    }

    /**
     * partition maxvalue
     */
    public static class MaxValue extends Expression implements LeafExpression {
        public static MaxValue INSTANCE = new MaxValue();

        @Override
        public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
            throw new UnsupportedOperationException("Unsupported for MaxValue");
        }

        @Override
        public boolean nullable() {
            return false;
        }

        @Override
        protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
            return this;
        }
    }
}

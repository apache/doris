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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.analyzer.identifier.TableIdentifier;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Builder for UnboundRelation
 */
public class UnboundRelationBuilder {
    private List<String> nameParts;
    private Optional<GroupExpression> groupExpression = Optional.empty();
    private Optional<LogicalProperties> logicalProperties = Optional.empty();
    private List<String> partitionNames;

    public UnboundRelationBuilder() {
    }

    public UnboundRelationBuilder(UnboundRelation r) {
        this.nameParts = r.getNameParts();
        this.groupExpression = r.getGroupExpression();
        this.logicalProperties = Optional.of(r.getLogicalProperties());
        this.partitionNames = r.getPartitionNames();
    }

    /**
     *  Set name.
     */
    public UnboundRelationBuilder setNameParts(List<String> nameParts) {
        this.nameParts = nameParts;
        return this;
    }

    /**
     *  Set name.
     */
    public UnboundRelationBuilder setNameParts(TableIdentifier identifier) {
        this.nameParts = Lists.newArrayList();
        if (identifier.getDatabaseName().isPresent()) {
            nameParts.add(identifier.getDatabaseName().get());
        }
        nameParts.add(identifier.getTableName());
        return this;
    }

    public UnboundRelationBuilder setGroupExpression(Optional<GroupExpression> groupExpression) {
        this.groupExpression = groupExpression;
        return this;
    }

    public UnboundRelationBuilder setLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        this.logicalProperties = logicalProperties;
        return this;
    }

    public UnboundRelationBuilder setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
        return this;
    }

    public UnboundRelation build() {
        return new UnboundRelation(nameParts, groupExpression, logicalProperties, partitionNames);
    }
}

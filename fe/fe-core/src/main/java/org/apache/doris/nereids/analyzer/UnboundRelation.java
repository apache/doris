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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeaf;
import org.apache.doris.nereids.util.Utils;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Represent a relation plan node that has not been bound.
 */
public class UnboundRelation extends LogicalLeaf {
    private final List<String> nameParts;

    public UnboundRelation(List<String> nameParts) {
        super(NodeType.LOGICAL_UNBOUND_RELATION);
        this.nameParts = nameParts;
    }

    /**
     * Constructor for UnboundRelation.
     *
     * @param identifier relation identifier
     */
    public UnboundRelation(TableIdentifier identifier) {
        super(NodeType.LOGICAL_UNBOUND_RELATION);
        this.nameParts = Lists.newArrayList();
        if (identifier.getDatabaseName().isPresent()) {
            nameParts.add(identifier.getDatabaseName().get());
        }
        nameParts.add(identifier.getTableName());
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public String getTableName() {
        return nameParts.stream().map(Utils::quoteIfNeeded)
                .reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public List<Slot> getOutput() throws UnboundException {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        return "UnresolvedRelation" + "(" + StringUtils.join(nameParts, ".") + ")";
    }
}

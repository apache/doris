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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.trees.NodeType;

import java.util.List;

/**
 * Abstract class for all physical scan node.
 */
public abstract class PhysicalScan extends PhysicalPlan {
    protected final Table table;
    protected final List<String> qualifier;

    /**
     * Constructor for PhysicalScan.
     *
     * @param type node type
     * @param table scan table
     * @param qualifier table's name
     */
    public PhysicalScan(NodeType type, Table table, List<String> qualifier) {
        super(type);
        this.table = table;
        this.qualifier = qualifier;
    }

    public Table getTable() {
        return table;
    }
}

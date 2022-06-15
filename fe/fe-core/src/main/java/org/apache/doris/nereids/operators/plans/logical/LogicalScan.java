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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.operators.OperatorType;

import java.util.List;
import java.util.Objects;

/**
 * Abstract class for logical scan plan operator.
 */
public abstract class LogicalScan extends LogicalLeafOperator {
    protected final List<String> qualifier;

    /**
     * Constructor for LogicalScan.
     *
     * @param type      node type
     * @param qualifier table's name
     */
    public LogicalScan(OperatorType type, List<String> qualifier) {
        super(type);
        this.qualifier = Objects.requireNonNull(qualifier, "qualifier can not be null");
    }

    public List<String> getQualifier() {
        return qualifier;
    }
}

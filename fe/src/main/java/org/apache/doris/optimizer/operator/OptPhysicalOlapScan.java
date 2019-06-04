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

package org.apache.doris.optimizer.operator;

import com.google.common.base.Preconditions;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.optimizer.base.*;

import java.util.List;

public class OptPhysicalOlapScan extends OptPhysical {
    private List<OptColumnRef> outputColumns;
    private OlapTable table;

    public OptPhysicalOlapScan(OlapTable table, List<OptColumnRef> outputColumns) {
        super(OptOperatorType.OP_PHYSICAL_OLAP_SCAN);
        this.table = table;
        this.outputColumns = outputColumns;
    }

    @Override
    public OrderEnforcerProperty getChildReqdOrder(
            OptExpressionHandle handle, OrderEnforcerProperty reqdOrder, int childIndex) {
        Preconditions.checkArgument(false);
        return null;
    }

    @Override
    public DistributionEnforcerProperty getChildReqdDistribution(
            OptExpressionHandle handle, DistributionEnforcerProperty reqdDistribution, int childIndex) {
        Preconditions.checkArgument(false);
        return null;
    }

    @Override
    protected OptColumnRefSet deriveChildReqdColumns(
            OptExpressionHandle exprHandle, RequiredPhysicalProperty property, int childIndex) {
        Preconditions.checkArgument(false);
        return null;
    }
}

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

import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.List;
import java.util.Optional;

/**
    wrapper for FileScan used for lazy materialization
 */
public class PhysicalLazyMaterializeFileScan extends PhysicalFileScan {
    private PhysicalFileScan scan;
    private SlotReference rowId;

    public PhysicalLazyMaterializeFileScan(PhysicalFileScan scan, SlotReference rowId) {
        super(scan.getRelationId(), scan.getTable(), scan.getQualifier(), scan.getDistributionSpec(),
                Optional.empty(), scan.getLogicalProperties(), scan.selectedPartitions, scan.getTableSample(),
                scan.getTableSnapshot());
        this.scan = scan;
        this.rowId = rowId;
    }

    @Override
    public List<String> getQualifier() {
        return scan.getQualifier();
    }
}

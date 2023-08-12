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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.catalog.OlapTable;

import java.util.List;

/** OlapScan */
public interface OlapScan {

    OlapTable getTable();

    long getSelectedIndexId();

    List<Long> getSelectedPartitionIds();

    List<Long> getSelectedTabletIds();

    /** getScanTabletNum */
    default int getScanTabletNum() {
        List<Long> selectedTabletIds = getSelectedTabletIds();
        if (selectedTabletIds.size() > 0) {
            return selectedTabletIds.size();
        }

        OlapTable olapTable = getTable();
        int selectTabletNumInPartitions = getSelectedPartitionIds().stream()
                .map(olapTable::getPartition)
                .map(partition -> partition.getDistributionInfo().getBucketNum())
                .reduce(Integer::sum)
                .orElse(0);
        if (selectTabletNumInPartitions > 0) {
            return selectTabletNumInPartitions;
        }

        // all partition's tablet
        return olapTable.getAllPartitions()
                .stream()
                .map(partition -> partition.getDistributionInfo().getBucketNum())
                .reduce(Integer::sum)
                .orElse(0);
    }
}

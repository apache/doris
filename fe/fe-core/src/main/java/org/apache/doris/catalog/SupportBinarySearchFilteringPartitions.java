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

package org.apache.doris.catalog;

import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.rpc.RpcException;

import java.util.Map;

/**
 * SupportBinarySearchFilteringPartitions: this interface is used to support binary search filtering partitions
 */
public interface SupportBinarySearchFilteringPartitions extends TableIf {
    /**
     * get the origin partition info which maybe not sorted, the NereidsSortedPartitionsCacheManager will
     * sort this partitions and cache in frontend. you can save the partition's meta snapshot id in the
     * CatalogRelation and get the partitions by the snapshot id.
     */
    Map<?, PartitionItem> getOriginPartitions(CatalogRelation scan);

    /**
     * return the version of the partitions meta, if the version changed, we should skip the legacy sorted
     * partitions and reload it.
     */
    Object getPartitionMetaVersion(CatalogRelation scan) throws RpcException;

    /**
     * when the partition meta loaded? if the partition meta load too frequently, we will skip sort partitions meta
     * and will not use binary search to filtering partitions
     */
    long getPartitionMetaLoadTimeMillis(CatalogRelation scan);
}

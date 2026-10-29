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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * For example, the related table is partitioned by `date` and `region`, with the following 6 partitions
 * <p>
 * 20200101 beijing
 * 20200101 shanghai
 * 20200102 beijing
 * 20200102 shanghai
 * 20200103 beijing
 * 20200103 shanghai
 * <p>
 * If the MTMV is partitioned by `date`, then the MTMV will have three partitions: 20200101, 202000102, 20200103
 * <p>
 * If the MTMV is partitioned by `region`, then the MTMV will have two partitions: beijing, shanghai
 */
public class MTMVRelatedPartitionDescOnePartitionColGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult, List<Column> partitionColumns,
                      Map<List<String>, Set<String>> queryUsedPartitionMap) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return;
        }
        Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> res = Maps.newHashMap();
        // the key is mv related table, the value is map<partition name, partition item>
        Map<MTMVRelatedTableIf, Map<String, PartitionItem>> relatedPartitionItems = lastResult.getItems();
        for (Entry<MTMVRelatedTableIf, Map<String, PartitionItem>> entry : relatedPartitionItems.entrySet()) {
            int relatedColPos = mvPartitionInfo.getPctColPos(entry.getKey());
            Set<String> queryUsedPartitions = queryUsedPartitionMap.get(entry.getKey().getFullQualifiers());
            Map<PartitionKeyDesc, Set<String>> onePctRes = Maps.newHashMap();
            for (Entry<String, PartitionItem> onePctEntry : entry.getValue().entrySet()) {
                // A partition of a range partitioned table is dropped here when the query does not read it:
                // its desc is its own bounds (such a table has one partition column, see
                // RangePartitionItem#toPartitionKeyDesc(int)), so one partition is one desc, and the filter
                // this reads is already expanded to MV partition granularity -- MTMVPartitionExpander,
                // through MTMV#calculatePartitionMappings -- so dropping it takes no bucket the query reads
                // out of the descs. It is what keeps the roll-up cheap: the roll-up parses, truncates and
                // increments a date per desc, and a view over a year's worth of daily partitions would
                // otherwise do that for all of them to answer a query about one day.
                if (onePctEntry.getValue() instanceof RangePartitionItem
                        && queryUsedPartitions != null && !queryUsedPartitions.contains(onePctEntry.getKey())) {
                    continue;
                }
                PartitionKeyDesc partitionKeyDesc = onePctEntry.getValue().toPartitionKeyDesc(relatedColPos);
                onePctRes.computeIfAbsent(partitionKeyDesc, k -> Sets.newHashSet()).add(onePctEntry.getKey());
            }
            // A list partitioned table keeps every partition's desc, and they are filtered out of the grouped
            // descs in MTMVRelatedPartitionDescTransferGenerator: a partition whose keys meet another table's
            // belongs to the MV partition this query is answered from even where the query does not read it,
            // and dropping it here would leave that desc short of its keys.
            res.put(entry.getKey(), onePctRes);
        }

        lastResult.setDescs(res);
    }
}

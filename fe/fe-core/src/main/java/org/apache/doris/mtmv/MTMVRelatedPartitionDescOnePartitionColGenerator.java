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
            RelatedPartitionDescResult lastResult, List<Column> partitionColumns) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return;
        }
        Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> res = Maps.newHashMap();
        Map<MTMVRelatedTableIf, Map<String, PartitionItem>> relatedPartitionItems = lastResult.getItems();
        for (Entry<MTMVRelatedTableIf, Map<String, PartitionItem>> entry : relatedPartitionItems.entrySet()) {
            int relatedColPos = mvPartitionInfo.getPctColPos(entry.getKey());
            Map<PartitionKeyDesc, Set<String>> onePctRes = Maps.newHashMap();
            for (Entry<String, PartitionItem> onePctEntry : entry.getValue().entrySet()) {
                PartitionKeyDesc partitionKeyDesc = onePctEntry.getValue().toPartitionKeyDesc(relatedColPos);
                if (onePctRes.containsKey(partitionKeyDesc)) {
                    onePctRes.get(partitionKeyDesc).add(onePctEntry.getKey());
                } else {
                    onePctRes.put(partitionKeyDesc, Sets.newHashSet(onePctEntry.getKey()));
                }
            }
            res.put(entry.getKey(), onePctRes);
        }

        lastResult.setDescs(res);
    }
}

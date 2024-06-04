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
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return;
        }
        Map<PartitionKeyDesc, Set<String>> res = Maps.newHashMap();
        Map<String, PartitionItem> relatedPartitionItems = lastResult.getItems();
        int relatedColPos = mvPartitionInfo.getRelatedColPos();
        for (Entry<String, PartitionItem> entry : relatedPartitionItems.entrySet()) {
            PartitionKeyDesc partitionKeyDesc = entry.getValue().toPartitionKeyDesc(relatedColPos);
            if (res.containsKey(partitionKeyDesc)) {
                res.get(partitionKeyDesc).add(entry.getKey());
            } else {
                res.put(partitionKeyDesc, Sets.newHashSet(entry.getKey()));
            }
        }
        lastResult.setDescs(res);
    }
}

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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Only focus on partial partitions of related tables
 */
public class MTMVRelatedPartitionDescSyncLimitGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        Map<Long, PartitionItem> partitionItems = lastResult.getItems();
        MTMVPartitionSyncConfig config = MTMVUtil.generateMTMVPartitionSyncConfigByProperties(mvProperties);
        if (config.getSyncLimit() <= 0) {
            return;
        }
        long nowTruncSubSec = MTMVUtil.getNowTruncSubSec(config.getTimeUnit(), config.getSyncLimit());
        Optional<String> dateFormat = config.getDateFormat();
        Map<Long, PartitionItem> res = Maps.newHashMap();
        int relatedColPos = mvPartitionInfo.getRelatedColPos();
        for (Entry<Long, PartitionItem> entry : partitionItems.entrySet()) {
            if (entry.getValue().isGreaterThanSpecifiedTime(relatedColPos, dateFormat, nowTruncSubSec)) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        lastResult.setItems(res);
    }
}

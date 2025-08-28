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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * get all related partition descs
 */
public class MTMVRelatedPartitionDescInitGenerator implements MTMVRelatedPartitionDescGeneratorService {
    private static final Logger LOG = LogManager.getLogger(MTMVRelatedPartitionDescInitGenerator.class);


    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        MTMVRelatedTableIf relatedTable = mvPartitionInfo.getRelatedTable();
        lastResult.setItems(relatedTable.getAndCopyPartitionItems(MvccUtil.getSnapshotFromContext(relatedTable)));
        if (LOG.isDebugEnabled()) {
            LOG.debug("related table: {}, partitions: {}",
                    relatedTable.getName(), lastResult.getItems());
        }
    }
}

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
import org.apache.doris.common.AnalysisException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Interface for materialized view partitioning function
 */
public interface MTMVPartitionExprService {

    /**
     * for list partition, get identity by expr
     *
     * @param partitionKeyDesc
     * @param mvProperties
     * @return
     * @throws AnalysisException
     */
    String getRollUpIdentity(PartitionKeyDesc partitionKeyDesc, Map<String, String> mvProperties)
            throws AnalysisException;

    /**
     * for range partition, get roll up PartitionKeyDesc by expr
     *
     * @param partitionKeyDesc
     * @param mvPartitionInfo
     * @return
     * @throws AnalysisException
     */
    PartitionKeyDesc generateRollUpPartitionKeyDesc(
            PartitionKeyDesc partitionKeyDesc, MTMVPartitionInfo mvPartitionInfo, MTMVRelatedTableIf pctTable)
            throws AnalysisException;

    /**
     * For range partition, a single base range partition may overlap multiple roll-up buckets
     * (e.g. {@code date_trunc(date_add(col, INTERVAL 3 HOUR), 'day')} on UTC-midnight base partitions),
     * so return all roll-up PartitionKeyDesc values that should be associated with the input range.
     *
     * <p>Default implementation keeps the historical 1-to-1 behavior.
     */
    default List<PartitionKeyDesc> generateRollUpPartitionKeyDescs(
            PartitionKeyDesc partitionKeyDesc, MTMVPartitionInfo mvPartitionInfo, MTMVRelatedTableIf pctTable)
            throws AnalysisException {
        return Collections.singletonList(generateRollUpPartitionKeyDesc(partitionKeyDesc, mvPartitionInfo, pctTable));
    }

    /**
     * Check if user input is legal
     *
     * @param mtmvPartitionInfo
     * @throws AnalysisException
     */
    void analyze(MTMVPartitionInfo mtmvPartitionInfo) throws AnalysisException;

    /**
     * for show create MTMV
     * @param mvPartitionInfo
     * @return
     */
    String toSql(MTMVPartitionInfo mvPartitionInfo);
}

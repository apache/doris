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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The table that implements this interface can serve as a partition table followed by MTMV
 */
public interface MTMVRelatedTableIf extends TableIf {

    /**
     * Get all partitions of the table
     *
     * @return partitionId->PartitionItem
     */
    Map<Long, PartitionItem> getPartitionItems();

    /**
     * Obtain the latest update time of partition data
     *
     * @param partitionId
     * @param item
     * @return millisecond
     * @throws AnalysisException
     */
    long getPartitionLastModifyTime(long partitionId, PartitionItem item) throws AnalysisException;

    /**
     * getPartitionType LIST/RANGE/UNPARTITIONED
     *
     * @return
     */
    PartitionType getPartitionType();

    /**
     * getPartitionColumnNames
     *
     * @return
     * @throws DdlException
     */
    Set<String> getPartitionColumnNames() throws DdlException;

    /**
     * Obtain the latest update time of table data
     *
     * @return
     * @throws AnalysisException
     */
    long getLastModifyTime() throws AnalysisException;

    /**
     * getPartitionColumns
     *
     * @return
     */
    List<Column> getPartitionColumns();
}

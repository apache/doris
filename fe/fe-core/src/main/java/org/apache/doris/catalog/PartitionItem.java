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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.common.AnalysisException;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

public abstract class PartitionItem implements Comparable<PartitionItem> {
    public static final Comparator<Map.Entry<Long, PartitionItem>> ITEM_MAP_ENTRY_COMPARATOR =
            Comparator.comparing(o -> ((ListPartitionItem) o.getValue()).getItems().iterator().next());

    // get the unique string of the partition item.
    public abstract String getItemsString();

    public abstract <T> T getItems();

    public abstract PartitionItem getIntersect(PartitionItem newItem);

    public boolean isDefaultPartition() {
        return false;
    }

    public abstract PartitionKeyDesc toPartitionKeyDesc();

    /**
     * Generate PartitionKeyDesc using only the posth PartitionValue
     *
     * @param pos
     * @return
     * @throws AnalysisException
     */
    public abstract PartitionKeyDesc toPartitionKeyDesc(int pos) throws AnalysisException;

    /**
     * Check if the partition meets the time requirements
     *
     * @param pos The position of the partition column to be checked in all partition columns
     * @param dateFormatOptional Convert other types to date format
     * @param nowTruncSubSec The time to compare
     * @return
     * @throws AnalysisException
     */
    public abstract boolean isGreaterThanSpecifiedTime(int pos, Optional<String> dateFormatOptional,
            long nowTruncSubSec)
            throws AnalysisException;


    //get the unique string of the partition item in sql format
    public abstract String getItemsSql();
}

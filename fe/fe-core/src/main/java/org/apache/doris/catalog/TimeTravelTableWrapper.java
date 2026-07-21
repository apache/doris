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

import com.google.common.collect.Maps;

import java.util.List;

/**
 * Marks a base-table scan used for time-travel reconstruction.
 *
 * <p>The wrapper keeps hidden history and TTL columns in the scan output. The ordinary BE TTL
 * policy is disabled for this scan; the logical time-travel plan applies TTL only after historical
 * rows have been reconstructed.
 */
public class TimeTravelTableWrapper extends OlapTableWrapper {

    public TimeTravelTableWrapper(OlapTable originTable) {
        super(originTable, originTable.getName(), originTable.getBaseSchema(true),
                originTable.getKeysType(), Maps.newHashMap());
    }

    @Override
    public boolean getEnableUniqueKeyMergeOnWrite() {
        return originTable.getEnableUniqueKeyMergeOnWrite();
    }

    @Override
    public List<Index> getIndexes() {
        return originTable.getIndexes();
    }

    @Override
    public boolean isColocateTable() {
        return originTable.isColocateTable();
    }

    @Override
    public boolean hasRowTtl() {
        return originTable.hasRowTtl();
    }
}

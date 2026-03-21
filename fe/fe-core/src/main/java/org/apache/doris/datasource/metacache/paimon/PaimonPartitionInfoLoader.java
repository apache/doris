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

package org.apache.doris.datasource.metacache.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.table.Table;

import java.util.List;

/**
 * Loads partition info for a snapshot projection from the base Paimon table and catalog metadata.
 */
public final class PaimonPartitionInfoLoader {
    private final PaimonTableLoader tableLoader;

    public PaimonPartitionInfoLoader(PaimonTableLoader tableLoader) {
        this.tableLoader = tableLoader;
    }

    public PaimonPartitionInfo load(NameMapping nameMapping, Table paimonTable, List<Column> partitionColumns)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return PaimonPartitionInfo.EMPTY;
        }
        try {
            List<Partition> paimonPartitions = tableLoader.catalog(nameMapping).getPaimonPartitions(nameMapping);
            boolean legacyPartitionName = PaimonUtil.isLegacyPartitionName(paimonTable);
            return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions, legacyPartitionName);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon partition info %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }
}

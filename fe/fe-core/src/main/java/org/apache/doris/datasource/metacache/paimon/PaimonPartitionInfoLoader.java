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
import org.apache.doris.datasource.paimon.PaimonReaderOptions;
import org.apache.doris.datasource.paimon.PaimonUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.Table;

import java.util.List;

/**
 * Loads typed partition metadata from the same snapshot-scoped table used to plan the query.
 */
public final class PaimonPartitionInfoLoader {
    public PaimonPartitionInfo load(NameMapping nameMapping, Table snapshotTable, List<Column> partitionColumns)
            throws AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return PaimonPartitionInfo.EMPTY;
        }
        try {
            // Catalog.listPartitions exposes path-oriented string specs. Paimon intentionally
            // maps null and blank strings to the same partition.default-name there, so those
            // specs cannot be used as logical partition identities. PartitionEntry retains the
            // typed BinaryRow and is also bound to the data snapshot represented by this table.
            PaimonReaderOptions.validateEffectivePlanningTable(snapshotTable);
            List<PartitionEntry> partitionEntries =
                    snapshotTable.newReadBuilder().newScan().listPartitionEntries();
            return PaimonUtil.generatePartitionInfo(snapshotTable, partitionColumns, partitionEntries);
        } catch (Exception e) {
            throw new CacheException("failed to load paimon partition info %s.%s.%s: %s",
                    e, nameMapping.getCtlId(), nameMapping.getLocalDbName(), nameMapping.getLocalTblName(),
                    e.getMessage());
        }
    }
}

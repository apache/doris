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

import org.apache.doris.common.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * A lightweight wrapper base for read binlog<Row> of table
 */
public class RowBinlogTableWrapper extends OlapTableWrapper {

    private final MaterializedIndexMeta rowBinlogMeta;

    public RowBinlogTableWrapper(OlapTable originTable) {
        this(originTable, Maps.newHashMap());
    }

    public RowBinlogTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap) {
        super(originTable, originTable.getName(), originTable.getRowBinlogMeta().getSchema(), KeysType.DUP_KEYS,
                partitionOffsetMap);
        this.rowBinlogMeta = originTable.getRowBinlogMeta();
        Preconditions.checkNotNull(rowBinlogMeta, "row binlog meta is null, table=%s", originTable.getName());
        this.setBaseIndexId(rowBinlogMeta.getIndexId());
    }

    @Override
    public long getBaseIndexId() {
        return rowBinlogMeta.getIndexId();
    }

    public static boolean isRowBinlogSyntheticColumn(Column column) {
        return column.getName().equals(Column.BINLOG_TSO_COL)
                || column.getName().equals(Column.BINLOG_LSN_COL);
    }

    @Override
    public MaterializedIndex getPartitionIndex(Partition partition, long indexId) {
        MaterializedIndex index = partition.getIndex(indexId);
        if (index != null) {
            return index;
        }
        // The row-binlog index meta does not exist as a partition index.
        // For scan range generation, reuse the base index's tablets.
        if (indexId == rowBinlogMeta.getIndexId()) {
            return partition.getIndex(originTable.getBaseIndexId());
        }
        return null;
    }

    @Override
    public KeysType getKeysType() {
        return KeysType.DUP_KEYS;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        RowBinlogTableWrapper other = (RowBinlogTableWrapper) obj;
        return rowBinlogMeta.equals(other.rowBinlogMeta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rowBinlogMeta.getRowBinlogIndexId());
    }
}

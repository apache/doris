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

import org.apache.doris.binlog.BinlogUtils;

import com.google.common.base.Preconditions;

/**
 * A lightweight wrapper base for read binlog<row> of table
 */
public class RowBinlogTableWrapper extends OlapTableWrapper {

    private final MaterializedIndexMeta rowBinlogMeta;

    public RowBinlogTableWrapper(OlapTable originTable) {
        super(originTable, BinlogUtils.wrapBinlogName(originTable.getName()),
                originTable.generateTableRowBinlogSchema(), KeysType.DUP_KEYS);
        this.rowBinlogMeta = originTable.getRowBinlogMeta();
        Preconditions.checkNotNull(rowBinlogMeta, "row binlog meta is null, table=%s", originTable.getName());
        this.setBaseIndexId(rowBinlogMeta.getIndexId());
    }

    @Override
    public long getBaseIndexId() {
        return rowBinlogMeta.getIndexId();
    }
}

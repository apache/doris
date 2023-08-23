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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;
import org.apache.doris.thrift.TStoragePolicyMetadataParam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StoragePolicyTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "storage_policy";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("StoragePolicyId", ScalarType.createStringType()),
            new Column("PartitionID", ScalarType.createStringType()));

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    private Long storagePolicyId;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    public StoragePolicyTableValuedFunction(Map<String, String> params) throws
            org.apache.doris.nereids.exceptions.AnalysisException {
        if (params.size() != 1) {
            throw new org.apache.doris.nereids
                .exceptions.AnalysisException("storage policy table-valued-function needs only one param");
        }
        Optional<Map.Entry<String, String>> opt = params.entrySet().stream()
                    .filter(entry -> entry.getKey().toLowerCase().equals("storage_policy")).findAny();
        if (!opt.isPresent()) {
            throw new org.apache.doris.nereids
                .exceptions.AnalysisException("storage policy table-valued-function needs storage_policy id");
        }
        storagePolicyId = Long.valueOf(opt.get().getValue());
    }

    @Override
    public String getTableName() {
        return NAME;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.STORAGE_POLICY;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.STORAGE_POLICY);
        TStoragePolicyMetadataParam storagePolicyMetadataParam = new TStoragePolicyMetadataParam();
        storagePolicyMetadataParam.setPolicyId(storagePolicyId);
        metaScanRange.setStoragePolicyParams(storagePolicyMetadataParam);
        return metaScanRange;
    }
}

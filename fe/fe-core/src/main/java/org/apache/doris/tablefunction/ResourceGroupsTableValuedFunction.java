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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * resource_groups().
 */
public class ResourceGroupsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "resource_groups";
    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX = new ImmutableMap.Builder<String, Integer>()
            .put("id", 0)
            .put("name", 1)
            .put("item", 2)
            .put("value", 3)
            .build();

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    public ResourceGroupsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() !=  0) {
            throw new AnalysisException("resource groups table-valued-function does not support any params");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.RESOURCE_GROUPS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.RESOURCE_GROUPS);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "ResourceGroupsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        List<Column> resColumns = Lists.newArrayList();
        resColumns.add(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)));
        resColumns.add(new Column("Name", ScalarType.createVarchar(64)));
        resColumns.add(new Column("Item", ScalarType.createVarchar(64)));
        resColumns.add(new Column("Value", ScalarType.createType(PrimitiveType.INT)));
        return resColumns;
    }
}

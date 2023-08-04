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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFrontendsMetadataParams;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * frontends_disks().
 */
public class FrontendsDisksTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "frontends_disks";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Name", ScalarType.createStringType()),
            new Column("Host", ScalarType.createStringType()),
            new Column("EditLogPort", ScalarType.createStringType()),
            new Column("DirType", ScalarType.createStringType()),
            new Column("Dir", ScalarType.createStringType()),
            new Column("Filesystem", ScalarType.createStringType()),
            new Column("Capacity", ScalarType.createStringType()),
            new Column("Used", ScalarType.createStringType()),
            new Column("Available", ScalarType.createStringType()),
            new Column("UseRate", ScalarType.createStringType()),
            new Column("MountOn", ScalarType.createStringType()));

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

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

    public FrontendsDisksTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (params.size() != 0) {
            throw new AnalysisException("frontends_disks table-valued-function does not support any params");
        }
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.FRONTENDS_DISKS;
    }

    @Override
    public TMetaScanRange getMetaScanRange() {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.FRONTENDS_DISKS);
        TFrontendsMetadataParams frontendsMetadataParams = new TFrontendsMetadataParams();
        frontendsMetadataParams.setClusterName("");
        metaScanRange.setFrontendsParams(frontendsMetadataParams);
        return metaScanRange;
    }

    @Override
    public String getTableName() {
        return "FrontendsDisksTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return SCHEMA;
    }
}


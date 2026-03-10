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
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * The implement of table valued function
 * authentication_integrations().
 */
public class AuthenticationIntegrationsTableValuedFunction extends MetadataTableValuedFunction {
    public static final String NAME = "authentication_integrations";

    private static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("IntegrationName", ScalarType.createStringType()),
            new Column("Type", ScalarType.createStringType()),
            new Column("Property", ScalarType.createStringType()),
            new Column("Value", ScalarType.createStringType()),
            new Column("Comment", ScalarType.createStringType())
    );

    private static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<String, Integer>();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public AuthenticationIntegrationsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        if (MapUtils.isNotEmpty(params)) {
            throw new AnalysisException("authentication_integrations table-valued-function does not support any "
                    + "params");
        }
    }

    public static Integer getColumnIndexFromColumnName(String columnName) {
        return COLUMN_TO_INDEX.get(columnName.toLowerCase());
    }

    @Override
    public String getTableName() {
        return "AuthenticationIntegrationsTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        return SCHEMA;
    }

    @Override
    public TMetadataType getMetadataType() {
        return TMetadataType.AUTHENTICATION_INTEGRATIONS;
    }

    @Override
    public TMetaScanRange getMetaScanRange(List<String> requiredFileds) {
        TMetaScanRange metaScanRange = new TMetaScanRange();
        metaScanRange.setMetadataType(TMetadataType.AUTHENTICATION_INTEGRATIONS);
        return metaScanRange;
    }
}

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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/*
 * SHOW PROC /dbs/dbId/tableId/index_schema/indexId"
 * show index schema
 */
public class IndexSchemaProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Field").add("Type").add("Null").add("Key")
            .add("Default").add("Extra")
            .build();

    private final List<Column> schema;
    private final Set<String> bfColumns;

    public IndexSchemaProcNode(List<Column> schema, Set<String> bfColumns) {
        this.schema = schema;
        this.bfColumns = bfColumns;
    }

    public static ProcResult createResult(List<Column> schema, Set<String> bfColumns) throws AnalysisException {
        Preconditions.checkNotNull(schema);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (Column column : schema) {
            // Extra string (aggregation and bloom filter)
            List<String> extras = Lists.newArrayList();
            if (column.getAggregationType() != null) {
                extras.add(column.getAggregationString());
            }
            if (bfColumns != null && bfColumns.contains(column.getName())) {
                extras.add("BLOOM_FILTER");
            }
            if (column.isAutoInc()) {
                extras.add("AUTO_INCREMENT");
            }
            if (column.getGeneratedColumnInfo() != null) {
                extras.add("STORED GENERATED");
            }
            String extraStr = StringUtils.join(extras, ",");

            List<String> rowList = Arrays.asList(column.getDisplayName(),
                                                 column.getOriginType().hideVersionForVersionColumn(true),
                                                 column.isAllowNull() ? "Yes" : "No",
                                                 ((Boolean) column.isKey()).toString(),
                                                 column.getDefaultValue() == null
                                                         ? FeConstants.null_string : column.getDefaultValue(),
                                                 extraStr);
            result.addRow(rowList);
        }
        return result;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return createResult(this.schema, this.bfColumns);
    }
}

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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.ColumnStat;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public class ShowColumnStatsStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("column_name")
                    .add(ColumnStat.NDV.getValue())
                    .add(ColumnStat.AVG_SIZE.getValue())
                    .add(ColumnStat.MAX_SIZE.getValue())
                    .add(ColumnStat.NUM_NULLS.getValue())
                    .add(ColumnStat.MIN_VALUE.getValue())
                    .add(ColumnStat.MAX_VALUE.getValue())
                    .build();

    private final TableName tableName;
    private final PartitionNames partitionNames;

    public ShowColumnStatsStmt(TableName tableName, PartitionNames partitionNames) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<String> getPartitionNames() {
        if (partitionNames == null) {
            return Collections.emptyList();
        }
        return partitionNames.getPartitionNames();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}

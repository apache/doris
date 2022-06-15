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

package org.apache.doris.qe;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.thrift.TColumnDefinition;
import org.apache.doris.thrift.TShowResultSet;
import org.apache.doris.thrift.TShowResultSetMetaData;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

// Result set of show statement.
// Redefine ResultSet now, because JDBC is too complicated.
// TODO(zhaochun): Maybe interface is better.
public class ShowResultSet extends AbstractResultSet {

    public ShowResultSet(ResultSetMetaData metaData, List<List<String>> resultRows) {
        super(metaData, resultRows);
    }

    public ShowResultSet(TShowResultSet resultSet) {
        List<Column> columns = Lists.newArrayList();
        for (int i = 0; i < resultSet.getMetaData().getColumnsSize(); i++) {
            TColumnDefinition definition = (TColumnDefinition) resultSet.getMetaData().getColumns().get(i);
            columns.add(new Column(
                            definition.getColumnName(),
                            ScalarType.createType(PrimitiveType.fromThrift(definition.getColumnType().getType())))
            );
        }
        this.metaData = new ShowResultSetMetaData(columns);
        this.resultRows = resultSet.getResultRows();
        this.rowIdx = -1;
    }

    public TShowResultSet tothrift() {
        TShowResultSet set = new TShowResultSet();
        set.metaData = new TShowResultSetMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Column definition = metaData.getColumn(i);
            set.metaData.addToColumns(new TColumnDefinition(
                    definition.getName(), definition.getOriginType().toColumnTypeThrift())
            );
        }

        set.resultRows = Lists.newArrayList();
        for (int i = 0; i < resultRows.size(); i++) {
            ArrayList<String> list = Lists.newArrayList();
            for (int j = 0; j < resultRows.get(i).size(); j++) {
                list.add(resultRows.get(i).get(j) == null ? FeConstants.null_string : resultRows.get(i).get(j));
            }
            set.resultRows.add(list);
        }
        return set;
    }
}

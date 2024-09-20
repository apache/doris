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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ShowDataTypesStmt extends ShowStmt implements NotFallbackInParser {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TypeName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(20)))
                    .build();

    public static ArrayList<PrimitiveType> getTypes() {
        return PrimitiveType.getSupportedTypes();
    }

    public static List<List<String>> getTypesAvailableInDdl() {
        ArrayList<PrimitiveType> supportedTypes = getTypes();
        List<List<String>> rows = Lists.newArrayList();
        for (PrimitiveType type : supportedTypes) {
            List<String> row = new ArrayList<>();
            if (type.isAvailableInDdl()) {
                row.add(type.toString());
                row.add(Integer.toString(type.getSlotSize()));
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ");
        sb.append("DATA TYPES");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void sortMetaData(List<List<String>> rows) {
        Collections.sort(rows, new Comparator<List<String>>() {
            @Override
            public int compare(List<String> row1, List<String> row2) {
                return row1.get(0).compareTo(row2.get(0));
            }
        });
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}

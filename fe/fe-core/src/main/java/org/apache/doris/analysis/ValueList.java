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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class ValueList {
    private List<ArrayList<Expr>> rows;

    public ValueList(ArrayList<Expr> row) {
        rows = Lists.newArrayList();
        rows.add(row);
    }

    public ValueList(List<ArrayList<Expr>> rows) {
        this.rows = rows;
    }

    public List<ArrayList<Expr>> getRows() {
        return rows;
    }

    public void addRow(ArrayList<Expr> row) {
        rows.add(row);
    }

    public ArrayList<Expr> getFirstRow() {
        return rows.get(0);
    }

    public void analyzeForSelect(Analyzer analyzer) throws AnalysisException {
        if (rows.isEmpty()) {
            throw new AnalysisException("No row in value list");
        }
        ArrayList<Expr> firstRow = null;
        int rowIdx = 0;
        for (ArrayList<Expr> row : rows) {
            rowIdx++;
            // 1. check number of fields if equal with first row
            if (firstRow != null && row.size() != firstRow.size()) {
                throw new AnalysisException("Column count doesn't match value count at row " + rowIdx);
            }
            for (int i = 0; i < row.size(); ++i) {
                Expr expr = row.get(i);
                if (expr instanceof DefaultValueExpr) {
                    throw new AnalysisException("Default expression can't exist in SELECT statement at row " + rowIdx);
                }
                expr.analyze(analyzer);
                if (firstRow != null) {
                    Type dstType = firstRow.get(i).getType();
                    if (!expr.getType().getPrimitiveType().equals(dstType.getPrimitiveType())) {
                        row.set(i, expr.castTo(dstType));
                    }
                }
            }
            if (firstRow == null) {
                firstRow = row;
            }
        }
    }
}

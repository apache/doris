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

import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;

// Column position used when add column
public class ColumnPosition {
    public static final ColumnPosition FIRST = new ColumnPosition();

    private String lastCol;

    public String getLastCol() {
        return lastCol;
    }

    // used to create FIRST position.
    private ColumnPosition() {
    }

    public ColumnPosition(String col) {
        this.lastCol = col;
    }

    public void analyze() throws AnalysisException {
        if (this == FIRST) {
            return;
        }
        if (Strings.isNullOrEmpty(lastCol)) {
            throw new AnalysisException("Column is empty.");
        }
    }

    public boolean isFirst() {
        return this == FIRST;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (this == FIRST) {
            sb.append("FIRST");
        } else {
            sb.append("AFTER `").append(lastCol).append("`");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

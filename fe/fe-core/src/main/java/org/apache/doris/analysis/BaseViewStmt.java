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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BaseViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(BaseViewStmt.class);

    protected final TableName tableName;
    protected final List<ColWithComment> cols;

    // Set during analyze
    protected final List<Column> finalCols;

    protected String inlineViewDef;


    public BaseViewStmt(TableName tableName, List<ColWithComment> cols) {
        this.tableName = tableName;
        this.cols = cols;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public TableName getTableName() {
        return tableName;
    }

    public List<Column> getColumns() {
        return finalCols;
    }

    public List<ColWithComment> getColWithComments() {
        return cols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    @Override
    public void analyze() throws AnalysisException, UserException {
    }
}

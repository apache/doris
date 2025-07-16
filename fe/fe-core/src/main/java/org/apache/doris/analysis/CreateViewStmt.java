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
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

@Deprecated
public class CreateViewStmt extends BaseViewStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(CreateViewStmt.class);

    private final boolean ifNotExists;
    private final boolean orReplace;
    private final String comment;

    public CreateViewStmt(boolean ifNotExists, boolean orReplace, TableName tableName, List<ColWithComment> cols,
            String comment) {
        super(tableName, cols);
        this.ifNotExists = ifNotExists;
        this.orReplace = orReplace;
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public boolean isSetOrReplace() {
        return orReplace;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }

    public void setInlineViewDef(String querySql) {
        inlineViewDef = querySql;
    }

    public void setFinalColumns(List<Column> columns) {
        finalCols.addAll(columns);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}

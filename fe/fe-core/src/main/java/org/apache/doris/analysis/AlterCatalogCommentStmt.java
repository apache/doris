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
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;

public class AlterCatalogCommentStmt extends AlterCatalogStmt implements NotFallbackInParser {
    private final String comment;

    public AlterCatalogCommentStmt(String catalogName, String comment) {
        super(catalogName);
        this.comment = comment;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(comment)) {
            throw new AnalysisException("New comment is not set.");
        }
    }

    @Override
    public String toSql() {
        return "ALTER CATALOG " + catalogName + " MODIFY COMMENT " + comment;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}

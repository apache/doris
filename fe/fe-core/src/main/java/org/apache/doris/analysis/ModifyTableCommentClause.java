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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// MODIFY COMMENT "new tbl comment";
public class ModifyTableCommentClause extends AlterTableClause {
    private static final Logger LOG = LogManager.getLogger(ModifyTableCommentClause.class);
    private String comment;

    public ModifyTableCommentClause(String comment) {
        super(AlterOpType.MODIFY_TABLE_COMMENT);
        this.comment = Strings.nullToEmpty(comment);
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY COMMENT ");
        sb.append("'").append(comment).append("'");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

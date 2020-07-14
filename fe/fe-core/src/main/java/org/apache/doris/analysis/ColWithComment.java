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
import org.apache.doris.common.FeNameFormat;

import com.google.common.base.Strings;

public class ColWithComment {

    private String colName;
    private String comment;

    public ColWithComment(String colName, String comment) {
        this.colName = colName;
        this.comment = Strings.nullToEmpty(comment);
    }

    public void analyze() throws AnalysisException {
        FeNameFormat.checkColumnName(colName);
    }

    public String getColName() {
        return colName;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public String toString() {
        String str = "`" + colName + "`";
        if (!comment.isEmpty()) {
            str += " COMMENT \"" + comment + "\"";
        }
        return str;
    }
}

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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Strings;

// Now only support utf-8
public class SetNamesVar extends SetVar {
    private static final String DEFAULT_NAMES = "utf8";
    private String charset;
    private String collate;

    public SetNamesVar(String charsetName) {
        this(charsetName, null);
    }

    public SetNamesVar(String charsetName, String collate) {
        this.charset = charsetName;
        this.collate = collate;
    }

    public String getCharset() {
        return charset;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(charset)) {
            charset = DEFAULT_NAMES;
        } else {
            charset = charset.toLowerCase();
        }
        // utf8-superset transform to utf8
        if (charset.startsWith(DEFAULT_NAMES)) {
            charset = DEFAULT_NAMES;
        }

        if (!charset.equalsIgnoreCase(DEFAULT_NAMES)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_CHARACTER_SET, charset);
        }
    }

    @Override
    public String toSql() {
        return "NAMES '" + charset + "' COLLATE "
                + (Strings.isNullOrEmpty(collate) ? "DEFAULT" : "'" + collate.toLowerCase() + "'");
    }

    @Override
    public String toString() {
        return toSql();
    }
}

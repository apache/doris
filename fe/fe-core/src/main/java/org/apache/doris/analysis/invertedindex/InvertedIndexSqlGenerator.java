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

package org.apache.doris.analysis.invertedindex;

import com.google.common.base.Strings;

public final class InvertedIndexSqlGenerator {
    private InvertedIndexSqlGenerator() {
    }

    /**
     * Builds the SQL fragment for USING ANALYZER clause.
     * Returns empty string if analyzer is null or empty.
     * Otherwise returns " USING ANALYZER <analyzer>" with proper quoting.
     */
    public static String buildAnalyzerSqlFragment(String analyzer) {
        if (Strings.isNullOrEmpty(analyzer)) {
            return "";
        }
        String trimmed = analyzer.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        if (trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return " USING ANALYZER " + trimmed;
        }
        return " USING ANALYZER '" + trimmed.replace("'", "''") + "'";
    }
}

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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.parser.NereidsParser;

import java.util.Map;
import java.util.TreeMap;

/**
 * NeedAuditEncryption
 */
public interface NeedAuditEncryption {
    boolean needAuditEncryption();

    /**
     * gene encryption SQL
     */
    default String geneEncryptionSQL(String sql) {
        if (!needAuditEncryption()) {
            return sql;
        }
        return maskSql(sql, false);
    }

    /** Apply audit masking rules, optionally retaining source formatting for FE logs. */
    static String maskSql(String sql, boolean preserveFormatting) {
        TreeMap<Pair<Integer, Integer>, String> indexInSqlToString = new TreeMap<>(new Pair.PairComparator<>());
        NereidsParser parser = new NereidsParser();
        parser.parseForEncryption(sql, indexInSqlToString, preserveFormatting);
        // parseForEncryption already returns UTF-16 offsets. Applying the view rewriter's
        // code-point conversion again shifts masks after supplementary characters such as emoji.
        StringBuilder result = new StringBuilder(sql.length());
        int offset = 0;
        for (Map.Entry<Pair<Integer, Integer>, String> entry : indexInSqlToString.entrySet()) {
            Pair<Integer, Integer> range = entry.getKey();
            result.append(sql, offset, range.first).append(entry.getValue());
            offset = range.second + 1;
        }
        return result.append(sql, offset, sql.length()).toString();
    }
}

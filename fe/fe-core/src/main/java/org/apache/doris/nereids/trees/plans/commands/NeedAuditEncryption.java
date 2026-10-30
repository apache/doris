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
import org.apache.doris.nereids.trees.plans.commands.info.BaseViewInfo;

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
        if (preserveFormatting) {
            // ANTLR indexes code points, while rewriteSql indexes UTF-16 chars. Advance through the
            // sorted ranges once so supplementary characters cannot leave part of a secret unmasked.
            TreeMap<Pair<Integer, Integer>, String> charRanges = new TreeMap<>(new Pair.PairComparator<>());
            int codePointOffset = 0;
            int charOffset = 0;
            for (Map.Entry<Pair<Integer, Integer>, String> entry : indexInSqlToString.entrySet()) {
                Pair<Integer, Integer> range = entry.getKey();
                int start = sql.offsetByCodePoints(charOffset, range.first - codePointOffset);
                int end = sql.offsetByCodePoints(start, range.second - range.first + 1);
                charRanges.put(Pair.of(start, end - 1), entry.getValue());
                codePointOffset = range.second + 1;
                charOffset = end;
            }
            return BaseViewInfo.rewriteSql(charRanges, sql);
        }
        return BaseViewInfo.rewriteSql(indexInSqlToString, sql);
    }
}

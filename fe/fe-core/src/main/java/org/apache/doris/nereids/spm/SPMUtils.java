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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * SPMUtils - common utility methods for SPM.
 *
 * Computes the structural digest and the structural hash. The digest is value-independent:
 * placeholders are rendered uniformly as _spm_const_var(id) / _spm_const_list(id),
 * so WHERE a = 100 and WHERE a = 42 produce the same digest (matching queries with
 * the same structure against one baseline); an IN-list placeholder is also independent
 * of the list length, so "IN (1, 2, 3)" matches "IN (10, 20)".
 */
public final class SPMUtils {

    private SPMUtils() {
    }

    /**
     * Computes the structural digest of a parameterized expression.
     *
     * Used for Level 2 exact digest matching. Column references are printed by their
     * real names (with an empty SQLRelation a SlotReference falls back to toSql()), and
     * placeholder values are normalized to "?".
     *
     * @param parameterized the already parameterized expression (contains SpmConstVar /
     *                      SpmConstList)
     * @return the structural digest text
     */
    public static String digest(Expression parameterized) {
        return new SPMExprSqlBuilder().print(parameterized, new SQLRelation());
    }

    /**
     * Computes the 64-bit structural hash (used for the Level 1 in-memory index,
     * design doc 6.1). Collisions are safe: Level 2 exact digest matching runs after
     * the hash filter.
     *
     * @param parameterized the already parameterized expression
     * @return a 64-bit structural hash
     */
    public static long hash(Expression parameterized) {
        return hashOf(digest(parameterized));
    }

    /**
     * Computes the 64-bit structural hash of a digest text (the full-query SPM digest
     * produced by Plan.toSpmDigest(), design doc 6.1).
     *
     * The key is derived with the same MD5 primitive the audit log uses
     * (DigestUtils.md5Hex), keeping the hash style consistent with the audit-log
     * sql_hash while remaining a VALUE-FREE structural key (the digest renders every
     * literal as "?" / _spm_const_var(id)). Staying value-free is required: this value
     * is the Level 1 bucket key (BaselineManager.hashIndex), and queries that differ
     * only in constants must land in the same bucket so the Level 2 exact digest match
     * is reached. ConnectContext.getSqlHash() cannot be used here - it hashes the
     * statement TEXT (the CREATE statement at creation time, the user statement with
     * literal values at rewrite time), so keying by it would break value-insensitive
     * matching.
     *
     * @param digestText the digest text to hash
     * @return a 64-bit structural hash
     */
    public static long hashOf(String digestText) {
        String md5 = DigestUtils.md5Hex(digestText);
        // 64 bits are enough: a bucket collision only costs one extra Level 2 digest
        // comparison, which stays authoritative.
        return Long.parseUnsignedLong(md5.substring(0, 16), 16);
    }
}

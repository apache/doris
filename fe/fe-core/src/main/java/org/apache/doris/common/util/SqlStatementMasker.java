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

package org.apache.doris.common.util;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Masks sensitive values in SQL before the statement leaves the execution path. */
public class SqlStatementMasker {
    public static final String MASKED_STATEMENT = "/* masked statement unavailable */";

    private static final Logger LOG = LogManager.getLogger(SqlStatementMasker.class);

    private SqlStatementMasker() {
    }

    /** Parse and mask a statement, failing closed when either step fails. */
    public static String mask(String statement) {
        if (statement == null || statement.isEmpty()) {
            return statement;
        }
        try {
            return maskInternal(statement, new NereidsParser().parseSingle(statement));
        } catch (Exception e) {
            LOG.warn("failed to prepare masked SQL statement, exception type: {}",
                    e.getClass().getSimpleName());
            return MASKED_STATEMENT;
        }
    }

    /** Mask a statement using its parsed logical plan, failing closed when masking fails. */
    public static String mask(String statement, LogicalPlan logicalPlan) {
        if (statement == null || statement.isEmpty()) {
            return statement;
        }
        try {
            return maskInternal(statement,
                    Preconditions.checkNotNull(logicalPlan, "logical plan must not be null"));
        } catch (Exception e) {
            LOG.warn("failed to mask SQL statement, exception type: {}", e.getClass().getSimpleName());
            return MASKED_STATEMENT;
        }
    }

    private static String maskInternal(String statement, LogicalPlan logicalPlan) {
        if (!(logicalPlan instanceof NeedAuditEncryption)) {
            return statement;
        }
        return ((NeedAuditEncryption) logicalPlan).geneEncryptionSQL(statement);
    }
}

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

package org.apache.doris.nereids.properties;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class SelectHintSetVar extends SelectHint {
    /**
     * Suppresses only parse-time SET_VAR assignments. Stored SPM SQL must retain hint nodes for matching
     * without applying another statement's settings to the current session. Planning a replay still applies
     * the caller's own assignments through applyToStatement.
     */
    private static final ThreadLocal<Boolean> PARSE_TIME_SET_VAR_SUPPRESSED = new ThreadLocal<>();

    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final Map<String, Optional<String>> parameters;

    public SelectHintSetVar(String hintName, Map<String, Optional<String>> parameters) {
        super(hintName);
        this.parameters = parameters;
    }

    /** Whether SET_VAR application is suppressed while the current thread builds parsed plans. */
    public static boolean isParseTimeSetVarSuppressed() {
        return Boolean.TRUE.equals(PARSE_TIME_SET_VAR_SUPPRESSED.get());
    }

    /**
     * Runs a parser callback without applying SET_VAR to the session. The parsed hint nodes remain
     * available for matching and later planning. Nested calls restore the enclosing suppression state.
     */
    public static <T> T withoutParseTimeSetVar(java.util.function.Supplier<T> action) {
        Boolean previous = PARSE_TIME_SET_VAR_SUPPRESSED.get();
        PARSE_TIME_SET_VAR_SUPPRESSED.set(Boolean.TRUE);
        try {
            return action.get();
        } finally {
            if (previous == null) {
                PARSE_TIME_SET_VAR_SUPPRESSED.remove();
            } else {
                PARSE_TIME_SET_VAR_SUPPRESSED.set(previous);
            }
        }
    }

    public Map<String, Optional<String>> getParameters() {
        return parameters;
    }

    /**
     * Applies these session-variable assignments for the current statement. Repeated calls apply the
     * assignments again; VariableMgr retains each variable's original value only once for statement cleanup.
     * Records the original parser mode before the first SQL-mode assignment.
     */
    public void applyToStatement(StatementContext context) {
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        // set temporary session value, and then revert value in the 'finally block' of StmtExecutor#execute
        sessionVariable.setIsSingleSetVar(true);
        for (Map.Entry<String, Optional<String>> kv : getParameters().entrySet()) {
            String key = kv.getKey();
            Optional<String> value = kv.getValue();
            if (value.isPresent()) {
                try {
                    if (SessionVariable.SQL_MODE.equalsIgnoreCase(key)) {
                        // The statement's TEXT is read under the mode in force when it STARTED
                        // to parse; this hint replaces the SESSION mode for the rest of the
                        // statement, so anything that re-parses the text (the SPM match key of
                        // a forwarded statement) or re-plans it on the master (the forwarded
                        // sql_mode) must be able to recover the PRE-hint mode. Captured here,
                        // before the write below, and only once per statement.
                        context.recordSpmOriginSqlMode(SqlModeHelper.currentMode());
                    }
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(value.get())));
                    context.invalidCache(key);
                } catch (Throwable t) {
                    throw new AnalysisException("Can not set session variable '"
                        + key + "' = '" + value.get() + "'", t);
                }
            }
        }
    }

    @Override
    public String toString() {
        String kvString = parameters
                .entrySet()
                .stream()
                .map(kv ->
                        kv.getValue().isPresent()
                        ? kv.getKey() + "='" + kv.getValue().get() + "'"
                        : kv.getKey()
                )
                .collect(Collectors.joining(", "));
        return super.getHintName() + "(" + kvString + ")";
    }
}

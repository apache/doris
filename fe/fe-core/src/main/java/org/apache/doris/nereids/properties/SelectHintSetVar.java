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
     * Thread-local guard: while a STORED SQL text (a persisted SPM baseline bind / plan
     * text) is being parsed, its SET_VAR payloads must NOT be applied by the PARSE itself.
     *
     * The parser applies a text's SET_VAR at parse time for the statement AT HAND
     * (setVarOnceInSql below, reverted by StmtExecutor at the statement's end). A stored
     * text is FOREIGN to the current statement: parsing it on the caller's thread used to
     * write the captured settings into the CALLER's session (a stored
     * SET_VAR(time_zone='+08:00') changed the zone during a baseline reconstruction and
     * returned different values even though the replayed tree had the hint stripped), and
     * the post-plan fingerprint parse repeated the write on every warm replay. The hint
     * node itself stays on the parsed tree, so a REPLAYED statement's own plan-time
     * application (EliminateLogicalSelectHint) is unaffected.
     */
    private static final ThreadLocal<Boolean> STORED_TEXT_PARSE = new ThreadLocal<>();

    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final Map<String, Optional<String>> parameters;

    public SelectHintSetVar(String hintName, Map<String, Optional<String>> parameters) {
        super(hintName);
        this.parameters = parameters;
    }

    /** Whether the CURRENT thread is parsing a stored SQL text (see runStoredTextParse). */
    public static boolean isStoredTextParse() {
        return Boolean.TRUE.equals(STORED_TEXT_PARSE.get());
    }

    /**
     * Runs one STORED SQL parse with the parse-time SET_VAR application suppressed (see
     * STORED_TEXT_PARSE). Re-entrant; the marker is restored afterwards.
     */
    public static <T> T runStoredTextParse(java.util.function.Supplier<T> action) {
        Boolean previous = STORED_TEXT_PARSE.get();
        STORED_TEXT_PARSE.set(Boolean.TRUE);
        try {
            return action.get();
        } finally {
            if (previous == null) {
                STORED_TEXT_PARSE.remove();
            } else {
                STORED_TEXT_PARSE.set(previous);
            }
        }
    }

    public Map<String, Optional<String>> getParameters() {
        return parameters;
    }

    /**
     * set session variable in sql level
     * @param context statement context
     */
    public void setVarOnceInSql(StatementContext context) {
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

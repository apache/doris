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

package org.apache.doris.nereids.processor.pre;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * eliminate set var hint, and set var to session variables.
 */
public class EliminateLogicalSelectHint extends PlanPreprocessor {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public LogicalPlan visitLogicalSelectHint(LogicalSelectHint<Plan> selectHint, StatementContext context) {
        for (Entry<String, Map<String, Optional<String>>> hint : selectHint.getHints().entrySet()) {
            String hintName = hint.getKey();
            if (hintName.equalsIgnoreCase("SET_VAR")) {
                setVar(hint.getValue(), context);
            } else {
                logger.warn("Can not process select hint '{}' and skip it", hint.getKey());
            }
        }

        return (LogicalPlan) selectHint.child();
    }

    private void setVar(Map<String, Optional<String>> parameters, StatementContext context) {
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        sessionVariable.setIsSingleSetVar(true);
        for (Entry<String, Optional<String>> kv : parameters.entrySet()) {
            String key = kv.getKey();
            Optional<String> value = kv.getValue();
            if (value.isPresent()) {
                try {
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(value.get())));
                } catch (Throwable t) {
                    throw new AnalysisException("Can not set session variable '" + key + "' = '"
                            + value.get() + "'", t);
                }
            }
        }
    }
}

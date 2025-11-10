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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.qe.VariableVarConverters;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * session variables global management rest api
 */
@RestController
public class VarsAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(VarsAction.class);

    /**
     * set global session variables for batch
     */
    @PutMapping("/api/_set_vars")
    public Object setVars(
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestBody List<Variable> vars
    ) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        if (CollectionUtils.isEmpty(vars)) {
            return ResponseEntityBuilder.badRequest("no vars set");
        }
        checkVarsNameNotNull(vars);
        checkVarsValueNotNull(vars);
        List<Variable> deduplicatedVariables = deduplicateVars(vars);
        List<SetVar> setVars = deduplicatedVariables.stream().map(variable ->
                new SetVar(
                        SetType.GLOBAL,
                        variable.getName(),
                        new StringLiteral(variable.getValue()),
                        SetVar.SetVarType.SET_SESSION_VAR
                )).collect(Collectors.toList());
        if (checkForwardToMaster(request)) {
            return forwardToMaster(request, vars);
        }
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setIsSingleSetVar(false);
        Map<SetVar, String> alreadySetVars = new LinkedHashMap<>();
        String firstInvalidVarName = null;
        try {
            // this can not guarantee acid, so we use switch to control acid and rollback if necessary
            // it may occur that partially successful, but interrupted when set invalid value format
            for (SetVar setVar : setVars) {
                // move it to current var name with every loop
                firstInvalidVarName = setVar.getVariable();
                VariableExpr variableExpr = new VariableExpr(setVar.getVariable(), setVar.getType());
                String originValue = VariableMgr.getValue(sessionVariable, variableExpr);
                VariableMgr.setVar(sessionVariable, setVar);
                alreadySetVars.put(setVar, originValue);
            }
        } catch (Exception e) {
            if (Config.enable_rollback_after_bulk_session_variables_set_failed) {
                // rollback for those vars that have been set value successful with their origin value
                rollbackModifiedVars(sessionVariable, alreadySetVars);
                String message = "vars value set failed because of invalid value for var: " + firstInvalidVarName;
                return ResponseEntityBuilder.badRequest(message);
            } else {
                List<String> successfullySetVarNames = alreadySetVars.keySet().stream()
                        .map(SetVar::getVariable).collect(Collectors.toList());
                List<Variable> failedToSetVarNames = vars.stream()
                        .filter(variable -> !successfullySetVarNames.contains(variable.getName()))
                        .collect(Collectors.toList());
                String message = "vars value set partially successful, successful vars is: " + successfullySetVarNames
                        +
                        ", failed vars is: " + failedToSetVarNames
                        +
                        ", invalid value for var is: " + firstInvalidVarName;
                return ResponseEntityBuilder.ok(message);
            }
        } finally {
            // that we can display the new set value by `show variables`
            VariableMgr.revertSessionValue(sessionVariable);
            // should we add synchronized lock for this method?
            sessionVariable.clearSessionOriginValue();
        }
        return ResponseEntityBuilder.ok(null);
    }

    /**
     * get global session variables for batch
     */
    @PostMapping("/api/_get_vars_values")
    public Object getVarsValues(
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestBody List<String> patterns
    ) throws Exception {
        executeCheckPassword(request, response);
        if (CollectionUtils.isEmpty(patterns)) {
            SessionVariable sessionVariable = VariableMgr.newSessionVariable();
            List<List<String>> rows = VariableMgr.dump(SetType.GLOBAL, sessionVariable, null);
            if (CollectionUtils.isEmpty(rows)) {
                return ResponseEntityBuilder.ok(Collections.emptyList());
            }
            List<Variable> result = new ArrayList<>(rows.size());
            Variable variable;
            for (List<String> row : rows) {
                if (CollectionUtils.isNotEmpty(row) && row.size() >= 4) {
                    variable = new Variable();
                    variable.setName(row.get(0));
                    variable.setValue(row.get(1));
                    variable.setDefaultValue(row.get(2));
                    variable.setChanged(row.get(3));
                    result.add(variable);
                }
            }
            return ResponseEntityBuilder.ok(result);
        }
        Set<String> deduplicatedPatterns = new HashSet<>(patterns);
        boolean existsNotAllowedVarName = deduplicatedPatterns.stream()
                .anyMatch(varName ->
                Objects.isNull(varName) || varName.isEmpty() || StringUtils.isBlank(varName));
        if (existsNotAllowedVarName) {
            return ResponseEntityBuilder.badRequest("null or empty var name is not allowed");
        }
        if (CollectionUtils.isEmpty(deduplicatedPatterns)) {
            return ResponseEntityBuilder.ok(new ArrayList<>());
        }
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        Set<String> matchedVarNames = new HashSet<>();
        Set<String> allVarNames = VariableMgr.getAllSessionVarNames();
        for (String pattern : deduplicatedPatterns) {
            if (pattern != null && !pattern.isEmpty()) {
                PatternMatcher matcher = PatternMatcher
                        .createMysqlPattern(pattern, CaseSensibility.VARIABLES.getCaseSensibility());
                for (String varName : allVarNames) {
                    if (matcher.match(varName)) {
                        matchedVarNames.add(varName);
                    }
                }
            }
        }
        if (matchedVarNames.isEmpty()) {
            return ResponseEntityBuilder.ok(Collections.emptyList());
        }
        List<Variable> result = new ArrayList<>();
        Variable variable;
        for (String varName : matchedVarNames) {
            VariableExpr variableExpr = new VariableExpr(varName, SetType.GLOBAL);
            String value = VariableMgr.getValue(sessionVariable, variableExpr);
            variable = new Variable();
            variable.setName(varName);
            String defaultValue = VariableMgr.getDefaultValue(varName);
            if (VariableVarConverters.hasConverter(varName)) {
                try {
                    variable.setValue(VariableVarConverters.decode(varName, Long.valueOf(value)));
                } catch (DdlException e) {
                    variable.setValue("");
                    LOG.warn("Decode session variable {} failed for value {}", varName, value, e);
                }
                try {
                    if (defaultValue != null) {
                        variable.setDefaultValue(VariableVarConverters.decode(varName, Long.valueOf(defaultValue)));
                    } else {
                        variable.setDefaultValue("");
                    }
                } catch (DdlException e) {
                    variable.setDefaultValue("");
                    LOG.warn("Decode session variable {} failed for default value {}", varName, defaultValue, e);
                }
            } else {
                variable.setValue(value);
                variable.setDefaultValue(defaultValue);
            }
            if (!variable.value.equals(variable.defaultValue)) {
                variable.setChanged("1");
            } else {
                variable.setChanged("0");
            }
            result.add(variable);
        }
        result.sort(Comparator.comparing(Variable::getName));
        return ResponseEntityBuilder.ok(result);
    }

    /**
     * unset global session variables for batch
     */
    @PutMapping("/api/_unset_vars")
    public Object unsetVars(
            HttpServletRequest request,
            HttpServletResponse response,
            @RequestBody List<String> varNames
    ) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        if (CollectionUtils.isEmpty(varNames)) {
            return ResponseEntityBuilder.badRequest("no vars set");
        }
        List<String> distinctVarNames = varNames.stream().distinct().collect(Collectors.toList());
        if (checkForwardToMaster(request)) {
            return forwardToMaster(request, varNames);
        }
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setIsSingleSetVar(false);
        try {
            // it should not occur exception when set vars with default value, so we do not need to guarantee acid hear
            for (String varName : distinctVarNames) {
                String defaultValue = VariableMgr.getDefaultValue(varName);
                SetVar var = new SetVar(SetType.GLOBAL, varName,
                        new StringLiteral(defaultValue), SetVar.SetVarType.SET_SESSION_VAR);
                VariableMgr.setVar(sessionVariable, var);
            }
        } finally {
            // that we can display the new set value by `show variables`
            VariableMgr.revertSessionValue(sessionVariable);
            // should we add synchronized lock for this method?
            sessionVariable.clearSessionOriginValue();
        }
        return ResponseEntityBuilder.ok(null);
    }

    private List<Variable> deduplicateVars(List<Variable> varNames) throws DdlException {
        if (CollectionUtils.isEmpty(varNames)) {
            return new ArrayList<>();
        }

        Map<String, List<Variable>> groupedByVarName = new LinkedHashMap<>();
        Set<Variable> duplicates = new HashSet<>();

        for (Variable var : varNames) {
            String name = var.getName();
            if (groupedByVarName.containsKey(name)) {
                List<Variable> existingVars = groupedByVarName.get(name);
                for (Variable existingVar : existingVars) {
                    if (!existingVar.getValue().equals(var.getValue())) {
                        duplicates.add(var);
                        duplicates.add(existingVar);
                    }
                }
                existingVars.add(var);
            } else {
                groupedByVarName.put(name, new ArrayList<>(Collections.singletonList(var)));
            }
        }

        if (!duplicates.isEmpty()) {
            throw new DdlException("duplicate variable name with different values: " + duplicates);
        }

        // Collect the final list of deduplicated variables
        List<Variable> deduplicatedVars = new ArrayList<>();
        for (List<Variable> vars : groupedByVarName.values()) {
            // Take the first occurrence of each variable
            deduplicatedVars.add(vars.get(0));
        }

        return deduplicatedVars;
    }

    private void checkVarsNameNotNull(List<Variable> varNames) throws DdlException {
        boolean existNullVariableName = varNames.stream().anyMatch(v -> Objects.isNull(v.getName()));
        if (existNullVariableName) {
            throw new DdlException("variable name must be not null");
        }
    }

    private void checkVarsValueNotNull(List<Variable> varNames) throws DdlException {
        List<Variable> nullValueList = varNames.stream()
                .filter(v -> Objects.isNull(v.getValue())).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(nullValueList)) {
            throw new DdlException("variable value is null: " + nullValueList);
        }
    }

    private void rollbackModifiedVars(SessionVariable sessionVariable, Map<SetVar, String> alreadySetVars)
            throws DdlException {
        if (!alreadySetVars.isEmpty()) {
            List<SetVar> rollbackSetVars = alreadySetVars.entrySet()
                    .stream().map(entry -> {
                        SetVar setVar = entry.getKey();
                        String originValue = entry.getValue();
                        return new SetVar(
                                SetType.GLOBAL,
                                setVar.getVariable(),
                                new StringLiteral(originValue),
                                SetVar.SetVarType.SET_SESSION_VAR
                        );
                    })
                    .collect(Collectors.toList());
            // it should not occur exception when set vars with origin value, so we do not need to guarantee acid hear
            for (SetVar setVar : rollbackSetVars) {
                VariableMgr.setVar(sessionVariable, setVar);
            }
        }
    }

    /**
     * unset all global session variables
     */
    @PutMapping("/api/_unset_all_vars")
    public Object unsetAllVars(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws Exception {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
        if (checkForwardToMaster(request)) {
            return forwardToMaster(request, null);
        }
        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setIsSingleSetVar(false);
        try {
            VariableMgr.setAllVarsToDefaultValue(sessionVariable, SetType.GLOBAL);
        } finally {
            // that we can display the new set value by `show variables`
            VariableMgr.revertSessionValue(sessionVariable);
            // multi thread issue may occur hear?
            sessionVariable.clearSessionOriginValue();
        }
        return ResponseEntityBuilder.ok(null);
    }

    @Data
    @ToString
    private static class Variable {

        private String name;
        private String value;
        private String defaultValue;
        private String changed;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Variable variable = (Variable) o;
            return Objects.equals(name, variable.name) && Objects.equals(value, variable.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }

    }

}

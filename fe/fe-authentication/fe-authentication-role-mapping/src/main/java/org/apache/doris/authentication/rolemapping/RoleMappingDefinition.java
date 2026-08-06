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

package org.apache.doris.authentication.rolemapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Pure role-mapping definition.
 */
public final class RoleMappingDefinition {
    private final String name;
    private final String integrationName;
    private final List<RuleDefinition> rules;

    public RoleMappingDefinition(String name, String integrationName, List<RuleDefinition> rules) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName is required");
        this.rules = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rules, "rules is required")));
    }

    public String getName() {
        return name;
    }

    public String getIntegrationName() {
        return integrationName;
    }

    public List<RuleDefinition> getRules() {
        return rules;
    }

    public List<UnifiedRoleMappingCelEngine.Rule> toEngineRules() {
        List<UnifiedRoleMappingCelEngine.Rule> engineRules = new ArrayList<>(rules.size());
        for (RuleDefinition rule : rules) {
            engineRules.add(UnifiedRoleMappingCelEngine.Rule.of(rule.condition, rule.grantedRoles));
        }
        return engineRules;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RoleMappingDefinition)) {
            return false;
        }
        RoleMappingDefinition that = (RoleMappingDefinition) other;
        return name.equals(that.name)
                && integrationName.equals(that.integrationName)
                && rules.equals(that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, integrationName, rules);
    }

    /**
     * Pure role-mapping rule definition.
     */
    public static final class RuleDefinition {
        private final String condition;
        private final Set<String> grantedRoles;

        public RuleDefinition(String condition, Set<String> grantedRoles) {
            this.condition = Objects.requireNonNull(condition, "condition is required");
            this.grantedRoles = Collections.unmodifiableSet(new LinkedHashSet<>(
                    Objects.requireNonNull(grantedRoles, "grantedRoles is required")));
        }

        public String getCondition() {
            return condition;
        }

        public Set<String> getGrantedRoles() {
            return grantedRoles;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof RuleDefinition)) {
                return false;
            }
            RuleDefinition that = (RuleDefinition) other;
            return condition.equals(that.condition) && grantedRoles.equals(that.grantedRoles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(condition, grantedRoles);
        }
    }
}

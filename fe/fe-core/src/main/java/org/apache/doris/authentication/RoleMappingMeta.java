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

package org.apache.doris.authentication;

import org.apache.doris.authentication.rolemapping.RoleMappingDefinition;
import org.apache.doris.common.UserAuditMetadata;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Persistent metadata for ROLE MAPPING.
 */
public class RoleMappingMeta extends UserAuditMetadata implements Writable {
    @SerializedName(value = "n")
    private String name;
    @SerializedName(value = "i")
    private String integrationName;
    @SerializedName(value = "r")
    private List<RuleMeta> rules;
    @SerializedName(value = "c")
    private String comment;

    private RoleMappingMeta() {
        super();
        this.name = "";
        this.integrationName = "";
        this.rules = Collections.emptyList();
        this.comment = null;
    }

    public RoleMappingMeta(String name, String integrationName, List<RuleMeta> rules, String comment,
            String createUser, long createTime, String alterUser, long modifyTime) {
        super(createUser, createTime, alterUser, modifyTime);
        this.name = Objects.requireNonNull(name, "name can not be null");
        this.integrationName = Objects.requireNonNull(integrationName, "integrationName can not be null");
        this.rules = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rules,
            "rules can not be null")));
        this.comment = comment;
    }

    public static RoleMappingMeta fromCreateSql(String mappingName, String integrationName,
            List<RuleMeta> rules, String comment, String createUser) {
        long currentTime = System.currentTimeMillis();
        return new RoleMappingMeta(mappingName, integrationName, rules, comment,
                Objects.requireNonNull(createUser, "createUser can not be null"),
                currentTime, createUser, currentTime);
    }

    public String getName() {
        return name;
    }

    public String getIntegrationName() {
        return integrationName;
    }

    public List<RuleMeta> getRules() {
        return rules;
    }

    public String getComment() {
        return comment;
    }

    public RoleMappingDefinition toDefinition() {
        List<RoleMappingDefinition.RuleDefinition> definitionRules = new ArrayList<>(rules.size());
        for (RuleMeta rule : rules) {
            definitionRules.add(rule.toDefinition());
        }
        return new RoleMappingDefinition(name, integrationName, definitionRules);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static RoleMappingMeta read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), RoleMappingMeta.class);
    }

    /**
     * Persisted ROLE MAPPING rule.
     */
    public static final class RuleMeta {
        @SerializedName(value = "c")
        private String condition;
        @SerializedName(value = "g")
        private Set<String> grantedRoles;

        private RuleMeta() {
            this.condition = "";
            this.grantedRoles = Collections.emptySet();
        }

        public RuleMeta(String condition, Set<String> grantedRoles) {
            this.condition = Objects.requireNonNull(condition, "condition can not be null");
            this.grantedRoles = Collections.unmodifiableSet(new LinkedHashSet<>(
                    Objects.requireNonNull(grantedRoles, "grantedRoles can not be null")));
        }

        public String getCondition() {
            return condition;
        }

        public Set<String> getGrantedRoles() {
            return grantedRoles;
        }

        public RoleMappingDefinition.RuleDefinition toDefinition() {
            return new RoleMappingDefinition.RuleDefinition(condition, grantedRoles);
        }
    }
}

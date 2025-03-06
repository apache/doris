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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.EnableFeatureClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

/**
 * EnableFeatureOp
 */
public class EnableFeatureOp extends AlterTableOp {
    /**
     * Features
     */
    public enum Features {
        BATCH_DELETE,
        SEQUENCE_LOAD,
        UPDATE_FLEXIBLE_COLUMNS,
        UNKNOWN
    }

    private String featureName;
    private boolean needSchemaChange;
    private Features feature;
    private Map<String, String> properties;

    public EnableFeatureOp(String featureName) {
        this(featureName, null);
    }

    public EnableFeatureOp(String featureName, Map<String, String> properties) {
        super(AlterOpType.ENABLE_FEATURE);
        this.featureName = featureName;
        this.needSchemaChange = false;
        this.properties = properties;
    }

    public boolean needSchemaChange() {
        return needSchemaChange;
    }

    public Features getFeature() {
        return feature;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        switch (featureName.toUpperCase()) {
            case "BATCH_DELETE":
                this.needSchemaChange = true;
                this.feature = Features.BATCH_DELETE;
                break;
            case "SEQUENCE_LOAD":
                this.needSchemaChange = true;
                this.feature = Features.SEQUENCE_LOAD;
                if (properties == null || properties.isEmpty()) {
                    throw new AnalysisException("Properties is not set");
                }
                break;
            case "UPDATE_FLEXIBLE_COLUMNS":
                this.needSchemaChange = true;
                this.feature = Features.UPDATE_FLEXIBLE_COLUMNS;
                break;
            default:
                throw new AnalysisException("unknown feature name: " + featureName);
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new EnableFeatureClause(featureName, properties);
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ENABLE FEATURE \"").append(featureName).append("\"");
        if (properties != null && !properties.isEmpty()) {
            sb.append(" WITH PROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }
}
